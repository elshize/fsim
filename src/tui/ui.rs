#![cfg_attr(tarpaulin, skip)]

use crate::simulation::{ImStatus, Query, QueryStatus, Status};
use crate::tui::app::{App, Mode, QueriesView, View, Window};
use tui::{
    backend::Backend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph, Row, SelectableList, Table, Text},
    Frame,
};

fn format_query((query, status): &(Query, QueryStatus)) -> String {
    format!("{}: QID({}) {:?}", query.request, query.id, status)
}

fn render_query_list<'a, B: Backend>(
    frame: &mut Frame<B>,
    rect: Rect,
    block: Block,
    queries: &'a [(Query, QueryStatus)],
    selected: Option<usize>,
) {
    let items: Vec<_> = queries.iter().map(format_query).collect();
    let mut list = SelectableList::default()
        .block(block)
        .items(&items)
        .select(selected)
        .highlight_style(Style::default().modifier(Modifier::BOLD).fg(Color::Red))
        .highlight_symbol(">");
    frame.render(&mut list, rect);
}

fn render_query_details<'a, B: Backend>(
    frame: &mut Frame<B>,
    rect: Rect,
    block: Block,
    queries: &'a [(Query, QueryStatus)],
    selected: usize,
) {
    let (query, status) = &queries[selected];
    let text = [
        Text::raw(format!("{:#?}\n", query)),
        Text::raw(format!("{:#?}\n", status)),
    ];
    let mut paragraph = Paragraph::new(text.iter()).block(block);
    frame.render(&mut paragraph, rect);
}

fn render_logs<'a, B: Backend>(
    frame: &mut Frame<B>,
    rect: Rect,
    block: Block,
    logs: impl Iterator<Item = &'a String>,
    selected: Option<usize>,
) {
    let items: Vec<_> = logs.collect();
    let selected = selected.or_else(|| items.iter().enumerate().map(|(idx, _)| idx).last());
    let mut list = SelectableList::default()
        .block(block)
        .select(selected)
        .highlight_style(Style::default().fg(Color::Blue))
        .items(&items);
    frame.render(&mut list, rect);
}

fn render_stats<B: Backend>(
    frame: &mut Frame<B>,
    rect: Rect,
    block: Block,
    step: usize,
    status: &ImStatus,
) {
    let table = vec![
        vec![String::from("Step"), format!("{}", step)],
        vec![String::from("Time"), format!("{:?}", status.time())],
        vec![
            String::from("Entered"),
            format!("{:?}", status.queries_entered()),
        ],
        vec![
            String::from("Finished"),
            format!("{:?}", status.queries_finished()),
        ],
        vec![
            String::from("Incomplete"),
            format!("{:?}", status.queries_incomplete()),
        ],
        vec![
            String::from("Active"),
            format!("{:?}", status.queries_active()),
        ],
        vec![
            String::from("Throughput"),
            match status.time().as_secs_f32() {
                s if s == 0.0 => String::from("?"),
                s => format!("{}", (status.queries_finished() as f32) / s),
            },
        ],
    ];
    let mut table = Table::new(
        [""].iter(),
        table.into_iter().map(|v| Row::Data(v.into_iter())),
    )
    .header_style(Style::default().fg(Color::Yellow).modifier(Modifier::BOLD))
    .block(block)
    .widths(&[Constraint::Percentage(30), Constraint::Percentage(70)]);
    frame.render(&mut table, rect);
}

fn block(title: &str, mode: Option<Mode>) -> Block {
    let block = Block::default().title(title).borders(Borders::ALL);
    match mode {
        Some(Mode::ActivePane) => block
            .title_style(Style::default().fg(Color::Red).modifier(Modifier::BOLD))
            .border_style(Style::default().fg(Color::Red)),
        Some(Mode::Navigation) => block
            .title_style(Style::default().fg(Color::Yellow).modifier(Modifier::BOLD))
            .border_style(Style::default().fg(Color::Yellow)),
        None => block,
    }
}

impl<'a> App<'a> {
    fn draw_view<B: Backend>(
        &self,
        frame: &mut Frame<B>,
        view: View,
        rect: Rect,
        mode: Option<Mode>,
    ) {
        match view {
            View::ActiveQueries(view) => match view {
                QueriesView::List(selected) => {
                    render_query_list(
                        frame,
                        rect,
                        block("Active", mode),
                        &self.snapshot.active_queries,
                        selected,
                    );
                }
                QueriesView::Details(query_idx) => {
                    render_query_details(
                        frame,
                        rect,
                        block("Active", mode),
                        &self.snapshot.active_queries,
                        query_idx,
                    );
                }
            },
            View::FinishedQueries(view) => match view {
                QueriesView::List(selected) => {
                    render_query_list(
                        frame,
                        rect,
                        block("Finished", mode),
                        &self.snapshot.finished_queries,
                        selected,
                    );
                }
                QueriesView::Details(query_idx) => {
                    render_query_details(
                        frame,
                        rect,
                        block("Finished", mode),
                        &self.snapshot.finished_queries,
                        query_idx,
                    );
                }
            },
            View::Logs(selected) => {
                render_logs(
                    frame,
                    rect,
                    block("Logs", mode),
                    self.snapshot.logs.iter(),
                    selected,
                );
            }
            View::Stats => {
                render_stats(
                    frame,
                    rect,
                    block("Status", mode),
                    self.sim.step(),
                    self.sim.status(),
                );
            }
        }
    }

    /// Draw application window.
    pub fn draw<B: Backend>(&mut self, frame: &mut Frame<B>) {
        self.frames.clear();
        match self.window {
            Window::Maximized(view) => {
                self.draw_view(frame, view, frame.size(), Some(Mode::ActivePane));
                self.frames.set_frame(view, frame.size());
            }
            Window::Main(view, mode) => {
                let main_layout = Layout::default()
                    .direction(Direction::Horizontal)
                    .margin(0)
                    .constraints([Constraint::Percentage(40), Constraint::Percentage(60)].as_ref())
                    .split(frame.size());
                let left_layout = Layout::default()
                    .direction(Direction::Vertical)
                    .margin(0)
                    .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
                    .split(main_layout[0]);
                let right_layout = Layout::default()
                    .direction(Direction::Vertical)
                    .margin(0)
                    .constraints([Constraint::Length(11), Constraint::Percentage(50)].as_ref())
                    .split(main_layout[1]);
                self.frames.set_frame(View::Stats, right_layout[0]);
                self.frames.set_frame(View::Logs(None), right_layout[1]);
                self.frames
                    .set_frame(View::ActiveQueries(QueriesView::default()), left_layout[0]);
                self.frames.set_frame(
                    View::FinishedQueries(QueriesView::default()),
                    left_layout[1],
                );
                self.draw_view(
                    frame,
                    view.match_or(View::Stats),
                    right_layout[0],
                    match view {
                        View::Stats => Some(mode),
                        _ => None,
                    },
                );
                self.draw_view(
                    frame,
                    view.match_or(View::Logs(None)),
                    right_layout[1],
                    match view {
                        View::Logs(_) => Some(mode),
                        _ => None,
                    },
                );
                self.draw_view(
                    frame,
                    view.match_or(View::ActiveQueries(QueriesView::default())),
                    left_layout[0],
                    match view {
                        View::ActiveQueries(_) => Some(mode),
                        _ => None,
                    },
                );
                self.draw_view(
                    frame,
                    view.match_or(View::FinishedQueries(QueriesView::default())),
                    left_layout[1],
                    match view {
                        View::FinishedQueries(_) => Some(mode),
                        _ => None,
                    },
                );
            }
        }
    }
}
