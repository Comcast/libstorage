/**
* Copyright 2019 Comcast Cable Communications Management, LLC
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* SPDX-License-Identifier: Apache-2.0
*/
extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(IntoPoint)]
pub fn point_derive(input: TokenStream) -> TokenStream {
    // Parse the string representation
    let ast = parse_macro_input!(input as DeriveInput);

    // Build the impl
    let generated = impl_point(&ast, false);

    // Return the generated impl
    TokenStream::from(generated)
}

#[proc_macro_derive(IntoChildPoint)]
pub fn child_point_derive(input: TokenStream) -> TokenStream {
    // Parse the string representation
    let ast = parse_macro_input!(input as DeriveInput);

    // Build the impl
    let generated = impl_point(&ast, true);

    // Return the generated impl
    TokenStream::from(generated)
}

fn impl_point(ast: &DeriveInput, child: bool) -> TokenStream {
    let name = &ast.ident;
    match ast.data {
        syn::Data::Struct(ref data) => impl_struct_point_fields(name, &data.fields, child),
        syn::Data::Enum(ref data) => {
            println!("into_enum_point_fields called");
            impl_enum_point_fields(name, &data.variants.iter().collect())
        }
        _ => unimplemented!(),
    }
}

fn find_optional_type(field: syn::Field) -> Option<Ident> {
    match field.clone().ty {
        syn::Type::Path(p) => {
            if let Some(i) = p.path.segments.clone().into_iter().next() {
                match i.arguments {
                    syn::PathArguments::AngleBracketed(a) => {
                        //println!("{:?}", a);
                        for ty in a.args {
                            match ty {
                                syn::GenericArgument::Type(p2) => match p2 {
                                    syn::Type::Path(p2) => {
                                        if let Some(i2) =
                                            p2.path.segments.clone().into_iter().next()
                                        {
                                            return Some(i2.ident);
                                        } else {
                                            return None;
                                        }
                                    }
                                    _ => return None,
                                },
                                _ => return None,
                            }
                        }
                    }
                    _ => {
                        return None;
                    }
                }
                return None;
            } else {
                return None;
            }
        }
        _ => return None,
    }
}

// TODO: Merge this with find_optional_type()
fn find_optional_vec_type(field: syn::Field) -> Option<Ident> {
    match field.clone().ty {
        syn::Type::Path(p) => {
            if let Some(i) = p.path.segments.clone().into_iter().next() {
                match i.arguments {
                    syn::PathArguments::AngleBracketed(a) => {
                        for ty in a.args {
                            match ty {
                                syn::GenericArgument::Type(p2) => match p2 {
                                    syn::Type::Path(p2) => {
                                        if let Some(i2) =
                                            p2.path.segments.clone().into_iter().next()
                                        {
                                            match i2.arguments {
                                                syn::PathArguments::AngleBracketed(a2) => {
                                                    for ty2 in a2.args {
                                                        match ty2 {
                                                            syn::GenericArgument::Type(p3) => {
                                                                match p3 {
                                                                    syn::Type::Path(p3) => {
                                                                        if let Some(i3) = p3
                                                                            .path
                                                                            .segments
                                                                            .clone()
                                                                            .into_iter()
                                                                            .next()
                                                                        {
                                                                            return Some(i3.ident);
                                                                        } else {
                                                                            return None;
                                                                        }
                                                                    }
                                                                    _ => return None,
                                                                }
                                                            }
                                                            _ => return None,
                                                        }
                                                    } // ends for ty2
                                                }
                                                _ => return None,
                                            }
                                            return None;
                                        } else {
                                            return None;
                                        }
                                    }
                                    _ => return None,
                                },
                                _ => return None,
                            }
                        }
                    }
                    _ => {
                        return None;
                    }
                }
                return None;
            } else {
                return None;
            }
        }
        _ => return None,
    }
}

fn impl_struct_point_fields(name: &syn::Ident, fields: &syn::Fields, child: bool) -> TokenStream {
    let _bool: Ident = Ident::new("bool", Span::call_site());
    let bwc: Ident = Ident::new("BWC", Span::call_site());
    let f_64: Ident = Ident::new("f64", Span::call_site());
    let i_32: Ident = Ident::new("i32", Span::call_site());
    let i_64: Ident = Ident::new("i64", Span::call_site());
    let optional: Ident = Ident::new("Option", Span::call_site());
    let s: Ident = Ident::new("String", Span::call_site());
    let u_8: Ident = Ident::new("u8", Span::call_site());
    let u_16: Ident = Ident::new("u16", Span::call_site());
    let u_64: Ident = Ident::new("u64", Span::call_site());
    let uuid: Ident = Ident::new("Uuid", Span::call_site());
    let value: Ident = Ident::new("Value", Span::call_site());
    let _vec: Ident = Ident::new("Vec", Span::call_site());

    let mut result = Vec::new();
    for field in fields {
        let ident = &field.ident;
        let ident_type = match field.clone().ty {
            syn::Type::Path(p) => {
                if let Some(i) = p.path.segments.clone().into_iter().next() {
                    Some(i.ident)
                } else {
                    None
                }
            }
            _ => None,
        };

        // In the case of optional types like Option<String> we need to
        // find the second parameter or we won't know what to do below
        let angle_type: Option<Ident> = if let Some(i_type) = ident_type.clone() {
            if i_type == optional {
                find_optional_type(field.clone())
            } else {
                None
            }
        } else {
            None
        };

        let vec_angle_type: Option<Ident> = if let Some(i_type) = ident_type.clone() {
            if i_type == _vec {
                find_optional_type(field.clone())
            } else {
                None
            }
        } else {
            None
        };

        match ident_type {
            Some(i_type) => {
                if i_type == bwc {
                    result.push(quote! {
                        p.add_field(stringify!(#ident), TsValue::Long(self.#ident.average()));
                        p.add_field(format!("{}_total_weight_in_kb",stringify!(#ident)), TsValue::Long(self.#ident.total_weight_in_kb));
                        p.add_field(format!("{}_num_seconds",stringify!(#ident)), TsValue::Long(self.#ident.num_seconds));
                        p.add_field(format!("{}_num_occured",stringify!(#ident)), TsValue::Long(self.#ident.num_occured));
                    });
                } else if i_type == s {
                    result.push(quote! {
                        if !self.#ident.is_empty(){
                            p.add_tag(stringify!(#ident), TsValue::String(self.#ident.clone()));
                        }
                    });
                } else if i_type == i_32 {
                    result.push(quote! {
                        p.add_field(stringify!(#ident), TsValue::Integer(self.#ident));
                    });
                } else if i_type == i_64 {
                    result.push(quote! {
                        p.add_field(stringify!(#ident), TsValue::SignedLong(self.#ident));
                    });
                } else if i_type == uuid {
                    result.push(quote! {
                        p.add_field(stringify!(#ident), TsValue::String(self.#ident.to_string()));
                    });
                } else if i_type == u_8 {
                    result.push(quote! {
                        p.add_field(stringify!(#ident), TsValue::Byte(self.#ident));
                    });
                } else if i_type == u_16 {
                    result.push(quote! {
                        p.add_field(stringify!(#ident), TsValue::Short(self.#ident));
                    });
                } else if i_type == u_64 {
                    result.push(quote! {
                        p.add_field(stringify!(#ident), TsValue::Long(self.#ident));
                    });
                } else if i_type == f_64 {
                    result.push(quote! {
                        p.add_field(stringify!(#ident), TsValue::Float(self.#ident));
                    });
                } else if i_type == _bool {
                    result.push(quote! {
                        p.add_field(stringify!(#ident), TsValue::Boolean(self.#ident));
                    });
                } else if i_type == _vec {
                    match &vec_angle_type {
                        Some(ref vec_type) => {
                            if *vec_type == s {
                                result.push(quote! {
                                    p.add_tag(stringify!(#ident), TsValue::StringVec(
                                        self.#ident.clone()
                                    ));
                                });
                            } else if *vec_type == u_64 {
                                result.push(quote! {
                                    p.add_tag(stringify!(#ident), TsValue::LongVec(
                                        self.#ident.clone()
                                    ));
                                });
                            } else if *vec_type == uuid {
                                result.push(quote! {
                                    p.add_tag(stringify!(#ident), TsValue::StringVec(
                                        self.#ident.iter().map(|i| i.to_string()).collect::<Vec<String>>(),
                                    ));
                                });
                            } else {
                                //println!("vec found {} with inner: {:?}", i_type, vec_angle_type);
                            }
                        }
                        None => {
                            // Unable to identify this type
                            println!(
                                "Unable to identify vec type for {:?} {:?} {:?}",
                                ident, i_type, vec_angle_type
                            );
                        }
                    }
                } else if i_type == optional {
                    //println!("optional type: {:?} {:?} {:?}", ident, i_type, angle_type,);
                    match angle_type {
                        Some(option_type) => {
                            if option_type == s {
                                result.push(quote! {
                                    if let Some(ref s) = self.#ident{
                                        if !s.is_empty(){
                                            p.add_tag(stringify!(#ident),
                                                TsValue::String(s.clone()));
                                        }
                                    }
                                });
                            } else if option_type == _bool {
                                result.push(quote! {
                                    if self.#ident.is_some(){
                                        p.add_field(stringify!(#ident),
                                            TsValue::Boolean(self.#ident.unwrap()));
                                    }
                                });
                            } else if option_type == bwc {
                                result.push(quote! {
                                    if self.#ident.is_some(){
                                        let bwc_val = self.#ident.clone().unwrap();
                                        p.add_field(stringify!(#ident),
                                            TsValue::Long(bwc_val.average()));
                                    }
                                });
                            } else if option_type == i_32 {
                                result.push(quote! {
                                    if self.#ident.is_some(){
                                        p.add_field(stringify!(#ident),
                                            TsValue::Integer(self.#ident.unwrap()));
                                    }
                                });
                            } else if option_type == i_64 {
                                result.push(quote! {
                                    if self.#ident.is_some(){
                                        p.add_field(stringify!(#ident),
                                            TsValue::SignedLong(self.#ident.unwrap()));
                                    }
                                });
                            } else if option_type == uuid {
                                result.push(quote! {
                                    if self.#ident.is_some(){
                                        p.add_field(stringify!(#ident),
                                            TsValue::String(self.#ident.unwrap().to_string()));
                                    }
                                });
                            } else if option_type == u_64 {
                                result.push(quote! {
                                    if self.#ident.is_some(){
                                        p.add_field(stringify!(#ident),
                                            TsValue::Long(self.#ident.unwrap()));
                                    }
                                });
                            } else if option_type == f_64 {
                                result.push(quote! {
                                    if self.#ident.is_some(){
                                        p.add_field(stringify!(#ident),
                                            TsValue::Float(self.#ident.unwrap()));
                                    }
                                });
                            } else if option_type == _vec {
                                let inner_vec_angle_type: Option<Ident> =
                                    find_optional_vec_type(field.clone());
                                match &inner_vec_angle_type {
                                    Some(ref vec_type) => {
                                        if *vec_type == s {
                                            result.push(quote! {
                                                if self.#ident.is_some() {
                                            p.add_field(stringify!(#ident), TsValue::StringVec(self.#ident.clone().unwrap()));
                                                }
                                });
                                        } // TODO: add other types here
                                    }
                                    None => {
                                        // Unable to identify this type
                                        println!(
                                            "Unable to identify vec type for option_type = {:?} ident= {:?} i_type= {:?} vec_angle_type = {:?}",
                                            option_type, ident, i_type, inner_vec_angle_type
                                        );
                                    }
                                }
                            } else {
                                //println!("optional else: {:?}", option_type);
                            }
                        }
                        None => {
                            // Unable to identify this type
                            println!(
                                "Unable to identify optional type for {:?} {:?} {:?}",
                                ident, i_type, angle_type
                            );
                        }
                    }
                } else {
                    // Uncomment me to debug why some fields may be missing
                    //println!("else: {:?} {:?} {:?}", ident, i_type, field.clone().ty);
                }
            }
            None => {
                // Unable to identify this type
                println!("Unable to identify type for {:?}", ident);
            }
        }
    }
    if child {
        TokenStream::from(quote! {
            impl ChildPoint for #name {
                fn sub_point(&self, p: &mut TsPoint) {
                    #(#result)*
                }
            }
        })
    } else {
        TokenStream::from(quote! {
            impl IntoPoint for #name {
                fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
                    let mut p = TsPoint::new(name.unwrap_or("unknown"), is_time_series);
                    #(#result)*
                    vec![p]
                }
            }
        })
    }
}

fn impl_enum_point_fields(name: &syn::Ident, variants: &Vec<&syn::Variant>) -> TokenStream {
    let mut result = Vec::new();
    for variant in variants {
        let ident = &variant.ident;
        match variant.discriminant {
            Some((ref _eq, ref expr)) => {
                result.push(quote! {
                    &#name::#ident => #expr.into_point(&mut buff),
                });
            }
            None => {
                result.push(quote! {
                    &#name::#ident => #ident.into_point(&mut buff),
                });
            }
        }
    }
    TokenStream::from(quote! {
        impl IntoPoint for #name {
            fn into_point(&self, name: Option<&str>, is_time_series: bool) -> TsPoint {
                let mut p = TsPoint::new(point_name.unwrap_or("unknown"), is_time_series);
                match self {
                    #(#result)*
                }
                p
            }
        }
    })
}
