module Bytes = struct
  include Bytes
  external unsafe_int_of_bits : bytes -> int -> int -> int = "stub_int_of_bits" [@@noalloc]
end
