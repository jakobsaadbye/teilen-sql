import { assert } from "../src/utils.ts";
import { BASE_10_DIGITS, BASE_52_DIGITS, fracMid } from "../src/frac.ts";

const main = () => {
    fracMidBase10();
    fracMidBase52();
    fracMidAppendOnly();
    // appendOnly();
}

const fracMidBase10 = () => {

    const tests = [
        ["[", "]", "5"],
        ["5", "]", "8"],
        ["8", "]", "9"],
        ["9", "]", "95"],
        ["98", "]", "99"],
        ["99", "]", "995"],

        ["1", "2", "15"],
        ["1", "15", "13"],
        ["001", "001002", "001001"],
        ["001", "001001", "0010005"],

        ["[", "5", "3"],
        ["[", "3", "2"],
        ["[", "2", "1"],
        ["[", "1", "05"],
        ["[", "11", "105"],
        ["[", "111", "1105"],
        ["[", "05", "03"],
        ["[", "03", "02"],
        ["[", "02", "01"],
        ["[", "01", "005"],
        ["[", "001", "0005"],

        ["05", "1", "08"],
        ["055", "110", "08"],
        ["09", "1", "095"],
        ["099", "1", "0995"],
        ["0998", "1", "0999"],

        ["499", "5", "4995"],
        ["111", "1111", "11105"]
    ];

    for (const [a, b, wanted] of tests) {
        const given = fracMid(a, b, BASE_10_DIGITS);
        assert(typeof(given) === 'string');
        if (given === wanted) {
            console.log(`PASS "${a}", "${b}", "${wanted}"`);
        } else {
            console.log(`FAIL "${a}", "${b}", "${wanted}", ${given}`);
        }
    }
}

const fracMidBase52 = () => {
    const tests = [
        ["[", "]", "a"],
        ["V", "]", "l"],
        ["l", "]", "t"],
    ];

    for (const [a, b, wanted] of tests) {
        const given = fracMid(a, b, BASE_52_DIGITS);
        assert(typeof(given) === 'string');
        if (given === wanted) {
            console.log(`PASS "${a}", "${b}", "${wanted}"`);
        } else {
            console.log(`FAIL "${a}", "${b}", "${wanted}", ${given}`);
        }
    }
}

const fracMidAppendOnly = () => {
    const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    // Append all
    const items: [char: string, pos: string][] = [];
    let i = 0;
    for (const char of [...alphabet]) {
        const lastItem = items.length > 0 ? items[i - 1] : undefined;
        let pos = "";
        if (lastItem === undefined) { // First
            pos = fracMid("[", "]");
        } else {
            const [_, lastPos] = lastItem;
            pos = fracMid(`${lastPos}`,"]");
        }
        items.push([char, pos]);
        i++;
    }

    // Sort by position
    items.sort(([, posA], [, posB]) => posA < posB ? -1 : 1);

    // Check that we get back the alphabet
    i = 0;
    for (const [given, ] of items) {
        const wanted = alphabet[i];
        if (given === wanted) {
            console.log(`PASS "${wanted}", "${given}"`);
        } else {
            console.log(`FAIL "${wanted}", "${given}"`);
        }
        i++;
    }
}

const appendOnly = () => {
    const iterations = 20;
    let position = "A";
    for (let i = 0; i < iterations; i++) {
        const mid = fracMid(position, "]");
        console.log(`(${position}, ]) -> (${mid}, ])`);
        position = mid;
    }
}

main();
