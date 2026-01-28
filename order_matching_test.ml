(* 
brew install ocaml
opam init -y

eval "$(opam env)"

ocaml order_matching_test.ml

ocamlopt -o order_matching_test order_matching_test.ml
./order_matching_test
*)

(* OCaml Finance Demo: High-Frequency Order Validation & Matching *)

type side = Buy | Sell

type order = {
  id : int;
  symbol : string;
  price : float;
  quantity : int;
  side : side;
}

type account = {
  owner : string;
  balance : float;
  positions : (string * int) list; (* Symbol * Quantity *)
}

(* Result variant for safe error handling *)
type trade_result = 
  | Executed of float (* Total trade value *)
  | Rejected of string
  | InsufficientFunds

module RiskEngine = struct
  (* Use Pattern Matching to validate the trade against account rules *)
  let validate_order acc ord =
    match ord.side with
    | Buy ->
        let cost = ord.price *. float_of_int ord.quantity in
        if acc.balance >= cost then Executed cost else InsufficientFunds
    | Sell ->
        let current_pos = List.assoc_opt ord.symbol acc.positions |> Option.value ~default:0 in
        if current_pos >= ord.quantity then 
          Executed (ord.price *. float_of_int ord.quantity)
        else 
          Rejected "Short selling not permitted in this account type."
end

(* --- Execution Demo --- *)
let () =
  let my_account = { 
    owner = "Jane_Street_Trader"; 
    balance = 50000.0; 
    positions = [("ORCL", 100)] 
  } in

  let trade_1 = { id = 101; symbol = "ORCL"; price = 180.50; quantity = 50; side = Sell } in
  let trade_2 = { id = 102; symbol = "NVDA"; price = 600.00; quantity = 200; side = Buy } in

  (* Process Trade 1 (Sell ORCL) *)
  (match RiskEngine.validate_order my_account trade_1 with
  | Executed value -> Printf.printf "Trade 101 Success: Sold for $%.2f\n" value
  | Rejected msg -> Printf.printf "Trade 101 Failed: %s\n" msg
  | InsufficientFunds -> print_endline "Trade 101 Failed: Over Credit Limit");

  (* Process Trade 2 (Buy NVDA - Should fail) *)
  match RiskEngine.validate_order my_account trade_2 with
  | InsufficientFunds -> print_endline "Trade 102 Rejected: Insufficient Capital for NVDA purchase."
  | _ -> print_endline "Unexpected outcome."