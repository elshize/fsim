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
use syn::{parse_macro_input, parse_quote, parse_str, token, FieldsNamed, Ident, Type};

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

//#[proc_macro_derive(Simulation, attributes(events))]
//pub fn simulation_derive(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
//    let input = parse_macro_input!(tokens as DeriveInput);
//    handle!(simulation_derive2(input))
//}

//fn simulation_derive2(input: DeriveInput) -> syn::Result<TokenStream> {
//    let events = event_list(&input.attrs)?;
//    if input.generics.lt_token.is_some() {
//        return Err(syn::Error::new_spanned(
//            input.generics,
//            "Generic simulation wrapper structs are not supported at the moment.",
//        ));
//    }
//    let mut tokens = impl_run(&events, &input.ident)?;
//    tokens.extend(impl_add_component(&input.ident)?);
//    tokens.extend(impl_enum_trait(&events)?);
//    //tokens.extend(impl_new(&input.ident, &input.data)?);
//    Ok(tokens)
//}

fn impl_enum_trait(events: &[syn::Path], bounding_trait: &Ident) -> syn::Result<TokenStream> {
    let mut tokens = quote! {
        pub trait #bounding_trait {}
    };
    for event in events {
        tokens.extend(quote! {
            impl ValidEvent for #event {}
        });
    }
    Ok(tokens)
}

fn impl_add_component(ident: &Ident) -> syn::Result<TokenStream> {
    Ok(quote! {
        impl #ident {
            #[must_use]
            pub fn add_component<E: ValidEvent + 'static, C: Component<Event = E> + 'static>(
                &mut self,
                component: C,
            ) -> ComponentId<E> {
                let components = &mut self.components;
                let id = components.len();
                let component: Box<dyn Component<Event = E>> = Box::new(component);
                components.push(Box::new(component));
                ComponentId {
                    id,
                    _marker: PhantomData,
                }
            }
        }
    })
}

fn impl_add_queue(ident: &Ident) -> syn::Result<TokenStream> {
    Ok(quote! {
        impl #ident {
            #[must_use]
            pub fn add_queue<V: 'static>(&mut self) -> QueueId<V> {
                self.state.new_queue()
            }

            #[must_use]
            pub fn add_bounded_queue<V: 'static>(&mut self, capacity: usize) -> QueueId<V> {
                self.state.new_bounded_queue(capacity)
            }
        }
    })
}

fn impl_default(sim: &Ident) -> syn::Result<TokenStream> {
    Ok(quote! {
        impl ::std::default::Default for #sim {
            fn default() -> Self {
                Self {
                    state: State::default(),
                    scheduler: Scheduler::default(),
                    components: Vec::default(),
                }
            }
        }
    })
}

fn impl_schedule(sim: &Ident) -> syn::Result<TokenStream> {
    Ok(quote! {
        impl #sim {
            pub fn schedule<E: 'static>(
                &mut self,
                time: Duration,
                component: ComponentId<E>,
                event: E
            ) {
                self.scheduler.schedule(time, component, event);
            }
        }
    })
}

fn impl_run(events: &[syn::Path], ident: &Ident) -> syn::Result<TokenStream> {
    let mut expr_iter = events
        .iter()
        .map(|e| {
            let expr: syn::ExprIf = parse_quote! {
                if type_id == ::std::any::TypeId::of::<#e>() {
                    let component = self
                        .components[event.component]
                        .downcast_mut::<Box<dyn Component<Event = #e>>>()
                        .expect("HERE");
                    let component_id = ComponentId {
                        id: event.component,
                        _marker: ::std::marker::PhantomData,
                    };
                    let event = event.inner.downcast_mut::<#e>().unwrap();
                    component.process_event(
                        component_id,
                        event,
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
            pub fn run(&mut self) {
                while let Some(mut event) = self.scheduler.pop() {
                    let type_id = event.event_type;
                    #ifexpr
                }
            }
        }
    })
}

// fn event_list(attrs: &[syn::Attribute]) -> syn::Result<Vec<syn::Path>> {
//     let mut events = attrs.iter().filter_map(|attr| {
//         if attr.style != syn::AttrStyle::Outer {
//             return None;
//         }
//         let pat: syn::Path = parse_str("events").unwrap();
//         if attr.path != pat {
//             return None;
//         }
//         Some(attr)
//     });
//     if let Some(attr) = events.next() {
//         match attr.parse_meta()? {
//             syn::Meta::Path(path) => {
//                 Err(syn::Error::new_spanned(path, "Expected a list of types."))
//             }
//             syn::Meta::NameValue(value) => {
//                 Err(syn::Error::new_spanned(value, "Expected a list of types."))
//             }
//             syn::Meta::List(list) if list.nested.is_empty() => Err(syn::Error::new_spanned(
//                 list,
//                 "Expected a non-empty list of types.",
//             )),
//             syn::Meta::List(paths) => {
//                 let paths: syn::Result<Vec<_>> = paths
//                     .nested
//                     .iter()
//                     .map(|m| match m {
//                         syn::NestedMeta::Lit(lit) => {
//                             Err(syn::Error::new_spanned(lit, "Expected a type path."))
//                         }
//                         syn::NestedMeta::Meta(syn::Meta::Path(path)) => Ok(path.clone()),
//                         _ => unreachable!(),
//                     })
//                     .collect();
//                 if let Some(attr) = events.next() {
//                     Err(syn::Error::new_spanned(
//                         attr,
//                         "Attribute events can be defined only once.",
//                     ))
//                 } else {
//                     Ok(paths?)
//                 }
//             }
//         }
//     } else {
//         Err(syn::Error::new(
//             Span::call_site(),
//             "Simulation structure must define events attribute.",
//         ))
//     }
// }

struct Simulation {
    name: Ident,
    events: Vec<syn::Path>,
    bounding_trait: Ident,
}

fn get_path_or(error: &str, ty: Type) -> syn::Result<syn::Path> {
    match ty {
        Type::Path(syn::TypePath { qself: None, path }) => Ok(path),
        ty => Err(syn::Error::new_spanned(ty, error)),
    }
}

impl syn::parse::Parse for Simulation {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        let fields: FieldsNamed = input.parse()?;

        let events: Ident = parse_str("events")?;
        let bounding_trait: Ident = parse_str("bounding_trait")?;

        let mut valid_fields: std::collections::HashMap<Ident, Option<Type>> =
            vec![(events.clone(), None), (bounding_trait.clone(), None)]
                .into_iter()
                .collect();
        for f in fields.named {
            let ident = &f.ident.unwrap();
            if let Some(visited) = valid_fields.get_mut(ident) {
                if visited.is_some() {
                    return Err(syn::Error::new_spanned(ident, "Duplicated field."));
                } else {
                    *visited = Some(f.ty);
                }
            } else {
                return Err(syn::Error::new_spanned(
                    ident,
                    "Invalid field. Expected one of: name, events, bounding_trait, timeline",
                ));
            }
        }
        let event_tuple: syn::TypeTuple = match valid_fields
            .remove(&events)
            .flatten()
            .ok_or_else(|| syn::Error::new(Span::call_site(), "Missing field: events."))?
        {
            Type::Tuple(tuple) => Ok(tuple),
            ty => Err(syn::Error::new_spanned(
                ty,
                "Event list must be represented as a tuple type.",
            )),
        }?;
        let bounding_trait: Ident = get_path_or(
            "Must be identifier.",
            valid_fields
                .remove(&bounding_trait)
                .flatten()
                .ok_or_else(|| {
                    syn::Error::new(Span::call_site(), "Missing field: bounding_trait.")
                })?,
        )
        .and_then(|path| {
            if let Some(id) = path.get_ident() {
                Ok(id.clone())
            } else {
                Err(syn::Error::new_spanned(
                    path,
                    "Bounding trait must be an identifier.",
                ))
            }
        })?;
        Ok(Simulation {
            name,
            events: event_tuple
                .elems
                .into_iter()
                .map(|ty| get_path_or("Must be a type path.", ty))
                .collect::<syn::Result<Vec<_>>>()?,
            bounding_trait,
        })
    }
}

fn impl_sim_struct(sim: &Ident) -> syn::Result<TokenStream> {
    Ok(quote! {
        pub struct #sim {
            state: State,
            scheduler: Scheduler,
            components: Vec<Box<dyn Any>>,
        }
    })
}

/// Defines a type-safe simulation structure.
#[proc_macro]
pub fn define_simulation(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(tokens as Simulation);
    handle!(
        impl_sim_struct(&input.name),
        impl_default(&input.name),
        impl_schedule(&input.name),
        impl_run(&input.events, &input.name),
        impl_enum_trait(&input.events, &input.bounding_trait),
        impl_add_component(&input.name),
        impl_add_queue(&input.name)
    )
}
