extern crate ocaml_build;
extern crate prost_build;
pub fn main() -> std::io::Result<()> {
  // prost_build::compile_protos(&["src/clientserver.proto"],
  //                             &["src/"]).unwrap(); 

  // ocaml_build::Sigs::new("src/rust.ml").generate()
  Ok(())
}