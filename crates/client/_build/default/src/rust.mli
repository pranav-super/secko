(* Generated by ocaml-rs *)

open! Bigarray

(* file: lib.rs *)

type db
type lookup = Present of string | NotPresent
external init: string -> int -> db = "init"
external push: db -> string -> unit = "push"
external get: db -> string -> lookup = "get"
external dump: db -> 'keyval array = "dump"
