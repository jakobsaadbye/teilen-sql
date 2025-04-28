import { decodeHlc, encodeHlc, Hlc, newHlc, receiveHlc, sendHlc } from "@/src/hlc.ts";
import { assertNotEquals } from "jsr:@std/assert@0.221/assert-not-equals";
import { assertEquals } from "jsr:@std/assert@0.221/assert-equals";

Deno.test("Encode/decode hybrid-logical-clock", () => {
    const clock = newHlc();

    const encoded = encodeHlc(clock);
    const decoded = decodeHlc(encoded);

    assertEquals(decoded.pt, clock.pt);
    assertEquals(decoded.lt, clock.lt);
});

Deno.test("Send HLC - test clock drift", () => {
    let clock = newHlc();

    // Increment the physical-time 5ms to simulate NTP drift
    clock = sendHlc(clock);
    clock.pt += 5;

    // Doing a new wall-clock reading should now be behind clock.pt (pt <= clock.pt)
    // the logical clock part should thus increment by 1
    const newClock = sendHlc(clock);

    assertEquals(newClock.lt, 1);
});

Deno.test("Small node simulation HLC", () => {

    type Node = {
        clock: Hlc,
        events: Hlc[]
    }

    // Form a list of N nodes each with a hybrid-logical-clock
    const N          = 5;
    const iterations = 1000;

    const nodes = Array.from({ length: N }).map(_ => ({ clock: newHlc(), events: [] } as Node));

    // At each iteration one of the two things can happen:
    //   1. A node shares its clock with a random node
    //   2. A node creates a new event (sendEvent)
    for (let i = 0; i < iterations; i++) {
        const r = randIntBetween(0, 100);
        if (r < 50) {
            // Scenario 1 - Share clock from A -> B
            const nodeA = pickRandom(nodes);
            let nodeB = pickRandom(nodes);
            while (nodeA === nodeB) {
                // Pick a different node, if we happen to randomly pick ourselves again
                nodeB = pickRandom(nodes);
            }

            nodeB.clock = receiveHlc(nodeB.clock, nodeA.clock);
            nodeB.events.push(nodeB.clock);
        } else {
            // Scenario 2 - Create event
            const node = pickRandom(nodes);
            node.clock = sendHlc(node.clock);
            node.events.push(node.clock);
        }
    }

    // Assert that no two clock values are the same in each node
    for (const node of nodes) {
        const encodedEvents = node.events.map(event => encodeHlc(event));
        for (let i = 0; i < encodedEvents.length; i++) {
            for (let j = 0; j < encodedEvents.length; j++) {
                if (i === j) continue;
                const clockA = encodedEvents[i];
                const clockB = encodedEvents[j];
                assertNotEquals(clockA, clockB);
            }
        }
    }
});

const pickRandom = <T>(arr: T[]) => {
    const index = randIntBetween(0, arr.length - 1);
    return arr[index];
}

const randIntBetween = (a: number, b: number) => {
    const t = Math.random();
    return Math.round(a + t * (b - a));
}