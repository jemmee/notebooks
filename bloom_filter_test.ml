(* ocaml bloom_filter_test.ml *)

(* A Pure, Standalone Bloom Filter Implementation based on the The Ten Plagues from the Book of Exodus *)

module BloomFilter = struct
  type t = {
    bit_array : bool array;
    size : int;
    k : int;
  }

  (** [hash salt str] is a djb2 implementation. 
      Using a salt allows us to simulate multiple independent hash functions. *)
  let hash salt str =
    let h = ref 5381 in
    String.iter (fun c ->
      h := ((!h lsl 5) + !h) + Char.code c + salt
    ) str;
    !h

  (** [create size k] initializes a filter with [size] bits and [k] hash rounds. *)
  let create size k =
    { bit_array = Array.make size false; size; k }

  (** [add bf item] marks the bits corresponding to [item] as true. *)
  let add bf item =
    for i = 1 to bf.k do
      let index = abs (hash i item) mod bf.size in
      bf.bit_array.(index) <- true
    done

  (** [mem bf item] returns false if [item] is definitely not in the filter. *)
  let mem bf item =
    let rec check i =
      if i > bf.k then true
      else
        let index = abs (hash i item) mod bf.size in
        if bf.bit_array.(index) then check (i + 1)
        else false
    in
    check 1
end

(* --- Main Program --- *)
let () =
  (* Setup a filter with 200 slots and 4 hash functions *)
  let filter = BloomFilter.create 200 4 in
  
  (* Items from the Book of Exodus: The Ten Plagues *)
  let plagues = [
    "Blood"; "Frogs"; "Lice"; "Flies"; "Livestock"; 
    "Boils"; "Hail"; "Locusts"; "Darkness"; "Firstborn"
  ] in
  
  (* Register the plagues in the filter *)
  List.iter (BloomFilter.add filter) plagues;

  print_endline "--- Bloom Filter: Exodus Event Sentinel ---";

  (* Test items: some are plagues, some are normal weather/events *)
  let events = ["Blood"; "Rain"; "Locusts"; "Sunshine"; "Frogs"; "Wind"] in

  List.iter (fun event ->
    let status = if BloomFilter.mem filter event then
      "MATCH (Potential Plague Signature)" 
    else 
      "NO MATCH (Normal Event)" 
    in
    Printf.printf "Event: %-9s -> %s\n" event status
  ) events