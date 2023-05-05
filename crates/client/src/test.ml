(* open Secko_client *)

let db = Secko_client.Rust.init "127.0.0.1" 6359 in
  let _ = Secko_client.Rust.push db "sample value" in
  (* let retrieve = Secko_client.Rust.get db "abc" in
    Printf.printf("\n%s") retrieve; *) (* throws error bc not u64 *)
  let retrieve = Secko_client.Rust.get db "25" in
    match retrieve with 
    | Present(s) -> Printf.printf("\n%s\n") s;
    | NotPresent -> Printf.printf("\nNot Present\n"); (* yep *)
  let retrieve = Secko_client.Rust.get db "18380983780737452339" in
    match retrieve with 
    | Present(s) -> Printf.printf("\n%s\n") s; (* yep *)
    | NotPresent -> Printf.printf("\nNot Present\n");
  let dumped = Secko_client.Rust.dump db in
    Array.iter (fun (a, b) -> Printf.printf "\n%s %s\n" a b) dumped;
