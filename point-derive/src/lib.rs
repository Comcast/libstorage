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
#[macro_use]
extern crate quote;
extern crate syn;

use proc_macro::TokenStream;
use syn::{Ident, PathParameters, Ty};

#[proc_macro_derive(IntoPoint)]
pub fn point_derive(input: TokenStream) -> TokenStream {
    // Construct a string representation of the type definition
    let s = input.to_string();

    // Parse the string representation
    let ast = syn::parse_derive_input(&s).unwrap();

    // Build the impl
    let gen = impl_point(&ast, false);

    // Return the generated impl
    gen.parse().unwrap()
}

#[proc_macro_derive(IntoChildPoint)]
pub fn child_point_derive(input: TokenStream) -> TokenStream {
    // Construct a string representation of the type definition
    let s = input.to_string();

    // Parse the string representation
    let ast = syn::parse_derive_input(&s).unwrap();

    // Build the impl
    let gen = impl_point(&ast, true);

    // Return the generated impl
    gen.parse().unwrap()
}

fn impl_point(ast: &syn::DeriveInput, child: bool) -> quote::Tokens {
    let name = &ast.ident;
    match ast.body {
        syn::Body::Struct(ref data) => impl_struct_point_fields(name, data.fields(), child),
        syn::Body::Enum(ref data) => {
            println!("into_enum_point_fields called");
            impl_enum_point_fields(name, data)
        }
    }
}

fn find_optional_type(field: syn::Field) -> Option<Ident> {
    match field.clone().ty {
        Ty::Path(_, p) => {
            if let Some(i) = p.segments.clone().into_iter().next() {
                match i.parameters {
                    PathParameters::AngleBracketed(a) => {
                        for ty in a.types {
                            match ty {
                                Ty::Path(_, p2) => {
                                    if let Some(i2) = p2.segments.clone().into_iter().next() {
                                        return Some(i2.ident);
                                    } else {
                                        return None;
                                    }
                                }
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

fn impl_struct_point_fields(
    name: &syn::Ident,
    fields: &[syn::Field],
    child: bool,
) -> quote::Tokens {
    let mut result = Vec::new();
    for field in fields {
        let ident = &field.ident;
        let ident_type = match field.clone().ty {
            Ty::Path(_, p) => {
                if let Some(i) = p.segments.clone().into_iter().next() {
                    Some(i.ident)
                } else {
                    None
                }
            }
            _ => None,
        };
        let bwc = Ident::new("BWC");
        let s = Ident::new("String");
        let i_32 = Ident::new("i32");
        let i_64 = Ident::new("i64");
        let f_64 = Ident::new("f64");
        let u_8 = Ident::new("u8");
        let u_16 = Ident::new("u16");
        let u_64 = Ident::new("u64");
        let uuid = Ident::new("Uuid");
        let _bool = Ident::new("bool");
        let _value = Ident::new("Value");
        let vec = Ident::new("Vec");
        let optional = Ident::new("Option");

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
            if i_type == vec {
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
                } else if i_type == vec {
                    match &vec_angle_type {
                        Some(ref vec_type) => {
                            if *vec_type == s {
                                result.push(quote! {
                                    p.add_tag(stringify!(#ident), TsValue::Vector(
                                        self.#ident.iter().map(|i| TsValue::String(i.clone())).collect::<Vec<TsValue>>(),
                                    ));
                                });
                            } else if *vec_type == u_64 {
                                result.push(quote! {
                                    p.add_tag(stringify!(#ident), TsValue::Vector(
                                        self.#ident.iter().map(|i| TsValue::Long(*i)).collect::<Vec<TsValue>>(),
                                    ));
                                });
                            } else if *vec_type == uuid {
                                result.push(quote! {
                                    p.add_tag(stringify!(#ident), TsValue::Vector(
                                        self.#ident.iter().map(|i| TsValue::String(i.to_string())).collect::<Vec<TsValue>>(),
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
        quote! {
            impl ChildPoint for #name {
                fn sub_point(&self, p: &mut TsPoint) {
                    #(#result)*
                }
            }
        }
    } else {
        quote! {
            impl IntoPoint for #name {
                fn into_point(&self, name: Option<&str>) -> Vec<TsPoint> {
                    let mut p = TsPoint::new(name.unwrap_or("unknown"), true);
                    #(#result)*
                    vec![p]
                }
            }
        }
    }
}

fn impl_enum_point_fields(name: &syn::Ident, variants: &[syn::Variant]) -> quote::Tokens {
    let mut result = Vec::new();
    for variant in variants {
        let ident = &variant.ident;
        match variant.discriminant {
            Some(ref val) => {
                result.push(quote! {
                    &#name::#ident => #val.into_point(&mut buff),
                });
            }
            None => {
                result.push(quote! {
                    &#name::#ident => #ident.into_point(&mut buff),
                });
            }
        }
    }
    quote! {
        impl IntoPoint for #name {
            fn into_point(&self, name: Option<&str>) -> TsPoint {
                let mut p = TsPoint::new(point_name.unwrap_or("unknown"), true);
                match self {
                    #(#result)*
                }
                p
            }
        }
    }
}
