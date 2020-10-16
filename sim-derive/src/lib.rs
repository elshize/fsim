//! `Simulation` derive macro implementation.

#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]
#![doc(html_root_url = "https://docs.rs/id-derive/0.1.0")]

extern crate proc_macro;

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{parse_macro_input, parse_quote, token, Ident, Item};

macro_rules! handle {
    ($s:expr) => {
        proc_macro::TokenStream::from(match $s {
            Ok(tokens) => tokens,
            Err(err) => return ::proc_macro::TokenStream::from(err.to_compile_error()),
        })
    };
    ($($s:expr),*) => {{
        let mut tokens = ::proc_macro2::TokenStream::new();
        $(
            match $s {
                Ok(t) => {
                    tokens.extend(t);
                },
                Err(err) => {
                    return ::proc_macro::TokenStream::from(err.to_compile_error())
                },
            }
        )*
        return ::proc_macro::TokenStream::from(tokens);
    }};
}

fn impl_add_component(ident: &Ident, event_trait: &Ident) -> syn::Result<TokenStream> {
    Ok(quote! {
        impl #ident {
            /// Adds a new component.
            #[must_use]
            pub fn add_component<E: #event_trait + 'static, C: ::sim20::Component<Event = E> + 'static>(
                &mut self,
                component: C,
            ) -> ::sim20::ComponentId<E> {
                self.components.add_component(component)
            }
        }
    })
}

fn impl_add_queue(ident: &Ident) -> syn::Result<TokenStream> {
    Ok(quote! {
        impl #ident {
            /// Adds a new unbounded queue.
            #[must_use]
            pub fn add_queue<V: 'static>(&mut self) -> ::sim20::QueueId<V> {
                self.state.new_queue()
            }

            /// Adds a new bounded queue.
            #[must_use]
            pub fn add_bounded_queue<V: 'static>(&mut self, capacity: usize) -> ::sim20::QueueId<V> {
                self.state.new_bounded_queue(capacity)
            }
        }
    })
}

fn impl_default(sim: &Ident) -> syn::Result<TokenStream> {
    Ok(quote! {
        impl ::std::default::Default for #sim {
            fn default() -> Self {
                let state = ::sim20::State::default();
                let components = ::sim20::Components::new(&state);
                Self {
                    state,
                    components,
                    scheduler: ::sim20::Scheduler::default(),
                }
            }
        }
    })
}

fn impl_schedule(sim: &Ident, event_trait: &Ident) -> syn::Result<TokenStream> {
    Ok(quote! {
        impl #sim {
            /// Schedules a new event to be executed at time `time` in component `component`.
            pub fn schedule<E: #event_trait + 'static>(
                &mut self,
                time: ::std::time::Duration,
                component: ::sim20::ComponentId<E>,
                event: E
            ) {
                self.scheduler.schedule(time, component, event);
            }
        }
    })
}

fn impl_step(events: &[syn::Path], ident: &Ident) -> syn::Result<TokenStream> {
    let mut expr_iter = events
        .iter()
        .map(|e| {
            let expr: syn::ExprIf = parse_quote! {
                if let Some(event) = event.downcast::<#e>() {
                    let component = self.components.get_mut::<#e>(event.component_id);
                    log::trace!("[{:?}] [event] {:?}", self.scheduler.time(), event);
                    component.process_event(
                        event.component_id,
                        event.event,
                        &mut self.scheduler,
                        &mut self.state
                    );
                }
            };
            expr
        })
        .rev();
    let mut first = expr_iter
        .next()
        .expect("Must be at least one or else parsing would fail.");
    first.else_branch = Some((
        token::Else::default(),
        Box::new(parse_quote! {
            panic!("Invalid event. This means a bug in the macro code.")
        }),
    ));
    let ifexpr = expr_iter.fold(first, |acc, mut expr| {
        expr.else_branch = Some((token::Else::default(), Box::new(syn::Expr::If(acc))));
        expr
    });
    Ok(quote! {
        impl #ident {
            /// Performs one step of the simulation. Returns `true` if there was in fact an event
            /// available to process, and `false` instead, which signifies that the simulation
            /// ended.
            pub fn step(&mut self) -> bool {
                if let Some(mut event) = self.scheduler.pop() {
                    #ifexpr
                    true
                } else {
                    false
                }
            }
        }
    })
}

fn impl_run(events: &[syn::Path], ident: &Ident) -> syn::Result<TokenStream> {
    let mut expr_iter = events
        .iter()
        .map(|e| {
            let expr: syn::ExprIf = parse_quote! {
                if let Some(event) = event.downcast::<#e>() {
                    let component = self.components.get_mut::<#e>(event.component_id);
                    component.process_event(
                        event.component_id,
                        event.event,
                        &mut self.scheduler,
                        &mut self.state
                    );
                }
            };
            expr
        })
        .rev();
    let first = expr_iter
        .next()
        .expect("Must be at least one or else parsing would fail.");
    let ifexpr = expr_iter.fold(first, |acc, mut expr| {
        expr.else_branch = Some((token::Else::default(), Box::new(syn::Expr::If(acc))));
        expr
    });
    Ok(quote! {
        impl #ident {
            /// Runs the entire simulation from start to end.
            /// This function might not terminate if the end condition is not satisfied.
            pub fn run(&mut self) {
                while let Some(mut event) = self.scheduler.pop() {
                    #ifexpr
                }
            }
        }
    })
}

struct Definitions {
    items: Vec<Item>,
}

impl syn::parse::Parse for Definitions {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut items = Vec::new();
        while !input.is_empty() {
            items.push(input.parse()?);
        }
        Ok(Definitions { items })
    }
}

/// TODO
#[proc_macro]
pub fn simulation(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(tokens as Definitions);
    handle!(process_definitions(input))
}

fn find_definitions(input: &Definitions) -> syn::Result<((usize, usize), (usize, usize))> {
    let mut event_trait_idx: Option<(usize, usize)> = None;
    let mut sim_struct_idx: Option<(usize, usize)> = None;
    for (idx, item) in input.items.iter().enumerate() {
        match item {
            Item::Trait(trait_definition) => {
                let mut events = trait_definition
                    .attrs
                    .iter()
                    .enumerate()
                    .filter(|(_, attr)| {
                        attr.path.get_ident() == Some(&Ident::new("events", Span::call_site()))
                    });
                if let Some((attr_idx, event_attr)) = events.next() {
                    if event_trait_idx.is_some() {
                        return Err(syn::Error::new_spanned(
                            event_attr,
                            "Duplicated event attribute.",
                        ));
                    }
                    event_trait_idx = Some((idx, attr_idx));
                }
                if let Some((_, event_attr)) = events.next() {
                    return Err(syn::Error::new_spanned(
                        event_attr,
                        "Duplicated event attribute.",
                    ));
                }
            }
            Item::Struct(struct_definition) => {
                let mut sim = struct_definition
                    .attrs
                    .iter()
                    .enumerate()
                    .filter(|(_, attr)| {
                        attr.path.get_ident() == Some(&Ident::new("simulation", Span::call_site()))
                    });
                if let Some((attr_idx, attr)) = sim.next() {
                    if sim_struct_idx.is_some() {
                        return Err(syn::Error::new_spanned(
                            attr,
                            "Duplicated simulation attribute.",
                        ));
                    }
                    sim_struct_idx = Some((idx, attr_idx));
                }
                if let Some((_, attr)) = sim.next() {
                    return Err(syn::Error::new_spanned(
                        attr,
                        "Duplicated simulation attribute.",
                    ));
                }
            }
            _ => {}
        }
    }
    match (event_trait_idx, sim_struct_idx) {
        (None, _) => Err(syn::Error::new(
            Span::call_site(),
            "Must define the valid event trait with `events` attribute.",
        )),
        (_, None) => Err(syn::Error::new(
            Span::call_site(),
            "Must define simulation struct with `simulation` attribute.",
        )),
        (Some(e), Some(s)) => Ok((e, s)),
    }
}

fn parse_events(
    item: Item,
    attr_idx: usize,
) -> syn::Result<(Ident, Vec<syn::Path>, syn::ItemTrait)> {
    match item {
        Item::Trait(mut def) => {
            let attr = def.attrs[attr_idx].clone();
            let trait_path = def.ident.clone();
            let meta = attr.parse_meta()?;
            let event_paths: Vec<_> = match meta {
                syn::Meta::List(syn::MetaList { nested, .. }) => {
                    let paths: syn::Result<Vec<_>> = nested
                        .into_iter()
                        .map(|m| match m {
                            syn::NestedMeta::Meta(syn::Meta::Path(path)) => Ok(path),
                            _ => Err(syn::Error::new_spanned(
                                m,
                                "Events must be a list of types.",
                            )),
                        })
                        .collect();
                    paths?
                }
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "Events must be a list of types.",
                    ));
                }
            };
            def.attrs = def
                .attrs
                .into_iter()
                .enumerate()
                .filter_map(
                    |(idx, attr)| {
                        if idx == attr_idx {
                            None
                        } else {
                            Some(attr)
                        }
                    },
                )
                .collect();
            Ok((trait_path, event_paths, def))
        }
        _ => unreachable!(),
    }
}

fn parse_struct(item: Item, attr_idx: usize) -> syn::Result<(Ident, syn::ItemStruct)> {
    match item {
        Item::Struct(mut def) => {
            def.attrs = def
                .attrs
                .into_iter()
                .enumerate()
                .filter_map(
                    |(idx, attr)| {
                        if idx == attr_idx {
                            None
                        } else {
                            Some(attr)
                        }
                    },
                )
                .collect();
            Ok((def.ident.clone(), def))
        }
        _ => unreachable!(),
    }
}

fn impl_event_trait(events: &[syn::Path], bounding_trait: &Ident) -> syn::Result<TokenStream> {
    let mut tokens = TokenStream::new();
    for event in events {
        tokens.extend(quote! {
            impl #bounding_trait for #event {}
        });
    }
    Ok(tokens)
}

fn process_definitions(input: Definitions) -> syn::Result<TokenStream> {
    let ((events_idx, events_attr_idx), (sim_idx, sim_attr_idx)) = find_definitions(&input)?;
    let mut output = TokenStream::new();
    let mut event_trait = Ident::new("dummy", Span::call_site());
    let mut events: Vec<syn::Path> = Vec::new();
    let mut sim_ident = Ident::new("dummy", Span::call_site());
    for (idx, item) in input.items.into_iter().enumerate() {
        if idx == events_idx {
            let (event_trait_ident, event_list, trait_def) = parse_events(item, events_attr_idx)?;
            output.extend(quote! {#trait_def});
            output.extend(impl_event_trait(&event_list, &event_trait_ident)?);
            event_trait = event_trait_ident;
            events = event_list;
        } else if idx == sim_idx {
            let (sim, sim_struct) = parse_struct(item, sim_attr_idx)?;
            output.extend(quote! { #sim_struct });
            sim_ident = sim;
        } else {
            output.extend(quote! { #item });
        }
    }
    output.extend(impl_default(&sim_ident)?);
    output.extend(impl_run(&events, &sim_ident));
    output.extend(impl_schedule(&sim_ident, &event_trait));
    output.extend(impl_step(&events, &sim_ident));
    output.extend(impl_add_component(&sim_ident, &event_trait));
    output.extend(impl_add_queue(&sim_ident));
    Ok(output)
}
