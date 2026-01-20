"brew install gnu-smalltalk"
"gst gst BloomFilterTest.st"

"=================================================================
  Bloom Filter Test for GNU Smalltalk
  Data: Items from the Bible
 ================================================================="

Object subclass: BloomFilterTest [
    | bits size hashCount |

    BloomFilterTest class >> new: m hashes: k [
        ^self new initialize: m hashes: k
    ]

    initialize: m hashes: k [
        size := m.
        hashCount := k.
        "Create an array of zeros. GST uses 1-based indexing."
        bits := Array new: m.
        1 to: m do: [ :i | bits at: i put: 0 ].
    ]

    "Simple internal hash function mapping a string to an index"
    hash: aString seed: aSeed [
        | h |
        h := aSeed.
        aString do: [ :char |
            h := (h * 33 + char asInteger) bitAnd: 16rFFFFFFFF.
        ].
        ^(h rem: size) + 1
    ]

    add: anItem [
        1 to: hashCount do: [ :i |
            bits at: (self hash: anItem seed: i) put: 1.
        ].
    ]

    exists: anItem [
        1 to: hashCount do: [ :i |
            (bits at: (self hash: anItem seed: i)) = 0 ifTrue: [ ^false ].
        ].
        ^true
    ]
]

"--- Execution Script ---"

| filter additions tests |
filter := BloomFilterTest new: 100 hashes: 4.

"Add Artifacts"
additions := #('Ark' 'Manna' 'Aaron-Rod' 'Tabernacle' 'Stone-Tablets').
Transcript show: 'Adding Artifacts to the filter...'; cr.

additions do: [ :item | 
    filter add: item.
    Transcript show: ' > Added: ', item; cr.
].

Transcript cr; show: '--- Membership Tests ---'; cr.

"Test for items: some in the set, some not"
tests := #('Ark' 'Manna' 'Goliath' 'Golden-Calf' 'Frogs').

tests do: [ :item |
    | found |
    found := filter exists: item.
    Transcript show: 'Checking ', item, ': '.
    found 
        ifTrue: [ Transcript show: '[MAYBE PRESENT]'; cr ]
        ifFalse: [ Transcript show: '[DEFINITELY ABSENT]'; cr ].
].