// https://www.typescriptlang.org/play

// Define the specific locations in Jonah's journey
enum JonahLocation {
    Joppa = "JOPPA",
    Tarshish = "TARSHISH",
    GreatFish = "BELLY_OF_THE_FISH",
    Nineveh = "NINEVEH",
    EastOfCity = "EAST_OF_THE_CITY"
}

interface JonahEvent {
    location: JonahLocation;
    item: string;
    isDivineIntervention: boolean;
}

// Our Bloom Filter Class (Optimized for Jonah's journey)
class BloomFilterTest {
    private bitArray: Uint8Array;
    private size: number;

    constructor(size: number = 128) {
        this.size = size;
        this.bitArray = new Uint8Array(Math.ceil(size / 8));
    }

    private hash(input: string, seed: number): number {
        let hash = seed;
        for (let i = 0; i < input.length; i++) {
            hash = (hash << 5) - hash + input.charCodeAt(i);
            hash |= 0;
        }
        return Math.abs(hash) % this.size;
    }

    public add(event: JonahEvent): void {
        const key = `${event.location}-${event.item}`;
        // We use 3 different 'seeds' to simulate 3 hash functions
        [7, 13, 31].forEach(seed => {
            const idx = this.hash(key, seed);
            this.bitArray[Math.floor(idx / 8)] |= (1 << (idx % 8));
        });
    }

    public check(event: JonahEvent): boolean {
        const key = `${event.location}-${event.item}`;
        return [7, 13, 31].every(seed => {
            const idx = this.hash(key, seed);
            return (this.bitArray[Math.floor(idx / 8)] & (1 << (idx % 8))) !== 0;
        });
    }
}

// --- The Demo ---
const filter = new BloomFilterTest();

// Events that actually happened
filter.add({ location: JonahLocation.Joppa, item: "Ship", isDivineIntervention: false });
filter.add({ location: JonahLocation.GreatFish, item: "Prayer", isDivineIntervention: true });
filter.add({ location: JonahLocation.EastOfCity, item: "Gourd", isDivineIntervention: true });

// Testing the Filter
const test1 = filter.check({ location: JonahLocation.GreatFish, item: "Prayer", isDivineIntervention: true });
const test2 = filter.check({ location: JonahLocation.Tarshish, item: "Success", isDivineIntervention: false });

console.log(`Did Jonah pray in the fish? ${test1 ? "Probably Yes" : "Definitely No"}`);
console.log(`Did Jonah succeed in reaching Tarshish? ${test2 ? "Probably Yes" : "Definitely No"}`);