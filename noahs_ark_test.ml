(* ocaml noahs_ark_test.ml *)

(* Define a base class for an Animal *)
class animal (species_name : string) =
  object
    val species = species_name
    method get_species = species
    
    (* A virtual-like method to be overridden *)
    method speak = "..." 
  end

(* Inheritance: Specific types of animals *)
class lion =
  object
    inherit animal "Lion"
    method! speak = "Roar!"
  end

class sheep =
  object
    inherit animal "Sheep"
    method! speak = "Baaaa!"
  end

(* Define the Ark class *)
class ark =
  object (self)
    val mutable cargo = []
    val mutable rain_status = false

    method board (a : animal) =
      cargo <- a :: cargo;
      Printf.printf "[ARK] The %s has boarded.\n" a#get_species

    method start_rain =
      rain_status <- true;
      print_endline "\n[ARK] It has begun to rain. Closing the hatch."

    method roll_call =
      print_endline "\n--- Ark Roll Call ---";
      List.iter (fun a -> 
        Printf.printf "Species: %-6s | Sound: %s\n" a#get_species a#speak
      ) cargo
  end

(* --- The Story in Code --- *)
let () =
  let my_ark = new ark in
  
  (* Create animal instances *)
  let leo = new lion in
  let dolly = new sheep in

  (* Boarding process *)
  my_ark#board (leo :> animal);   (* The ':>' is 'Upcasting' to the base class *)
  my_ark#board (dolly :> animal);

  my_ark#start_rain;
  my_ark#roll_call