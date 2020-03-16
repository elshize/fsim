use crate::app::Component;
use fsim::{Query, QueryStatus};
use tui::{
    backend::Backend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph, Row, SelectableList, Table, Text, Widget},
    Frame,
};

fn draw_active<B: Backend>(frame: &mut Frame<B>, app: &crate::app::App, rect: Rect) {
    match &app.focus {
        Some(Component::ActiveDetails { queries, selected }) => {
            let (query, status) = &queries[*selected];
            let text = [Text::raw(format!("First line: {:#?}\n", status))];
            let title = format!("Active: {:?}", query);
            let mut paragraph = Paragraph::new(text.iter())
                .block(Block::default().title(&title).borders(Borders::ALL));
            frame.render(&mut paragraph, rect);
        }
        focus => {
            let mut render_list = |queries: &[(Query, QueryStatus)], selected: Option<usize>| {
                let items: Vec<_> = queries
                    .into_iter()
                    .map(|(q, s)| format!("{}: QID({}) {:?}", q.request, q.id, s))
                    .collect();
                let mut list = SelectableList::default()
                    .block(Block::default().title("Active").borders(Borders::ALL))
                    .items(&items)
                    .select(selected)
                    .highlight_style(Style::default().modifier(Modifier::BOLD).fg(Color::Red))
                    .highlight_symbol(">>");
                frame.render(&mut list, rect);
            };
            match &focus {
                Some(Component::Active { queries, selected }) => {
                    render_list(queries, Some(*selected));
                }
                _ => {
                    let mut queries: Vec<_> = app.sim.status().active().cloned().collect();
                    queries.sort_by_key(|&(Query { id: _, request }, _)| request);
                    render_list(&queries, None);
                }
            };
        }
    }
}

fn draw_summary<B: Backend>(frame: &mut Frame<B>, app: &crate::app::App, rect: Rect) {
    let table = vec![
        vec![String::from("Step"), format!("{}", app.sim.step())],
        vec![
            String::from("Time"),
            format!("{:?}", app.sim.status().time()),
        ],
        vec![
            String::from("Entered"),
            format!("{:?}", app.sim.status().queries_entered()),
        ],
        vec![
            String::from("Finished"),
            format!("{:?}", app.sim.status().queries_finished()),
        ],
        vec![
            String::from("Incomplete"),
            format!("{:?}", app.sim.status().queries_incomplete()),
        ],
        vec![
            String::from("Active"),
            format!("{:?}", app.sim.status().queries_active()),
        ],
    ];
    let mut table = Table::new(
        ["Status"].iter(),
        table.into_iter().map(|v| Row::Data(v.into_iter())),
    )
    .header_style(Style::default().fg(Color::Yellow).modifier(Modifier::BOLD))
    .block(Block::default().borders(Borders::ALL))
    .widths(&[Constraint::Percentage(30), Constraint::Percentage(70)]);
    frame.render(&mut table, rect);
}

fn draw_logs<B: Backend>(frame: &mut Frame<B>, app: &crate::app::App, rect: Rect) {
    let items: Vec<_> = app.sim.status().logs().collect();
    let mut list = SelectableList::default()
        .block(Block::default().title("Logs").borders(Borders::ALL))
        .select(items.iter().enumerate().map(|(idx, _)| idx).last())
        .items(&items);
    frame.render(&mut list, rect);
}

pub(super) fn draw<B: Backend>(frame: &mut Frame<B>, app: &crate::app::App) {
    let size = frame.size();
    match app.focus {
        Some(Component::Logs) => {
            draw_logs(frame, app, size);
        }
        _ => {
            let main_layout = Layout::default()
                .direction(Direction::Horizontal)
                .margin(0)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
                .split(size);
            let left_layout = Layout::default()
                .direction(Direction::Vertical)
                .margin(0)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
                .split(main_layout[0]);
            Block::default()
                .title("Finished")
                .borders(Borders::ALL)
                .render(frame, left_layout[1]);
            let right_layout = Layout::default()
                .direction(Direction::Vertical)
                .margin(0)
                .constraints([Constraint::Length(10), Constraint::Percentage(50)].as_ref())
                .split(main_layout[1]);
            draw_active(frame, app, left_layout[0]);
            draw_summary(frame, app, right_layout[0]);
            draw_logs(frame, app, right_layout[1]);
        }
    }
}
