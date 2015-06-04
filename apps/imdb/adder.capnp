@0xe309f28a2aa780e7;

# Cap'n Proto schema
interface Adder {
 add @0 (left :Int32, right :Int32) -> (value :Int32);
}