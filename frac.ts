import { assert } from "jsr:@std/assert@^0.217.0/assert";

/**
 * Implementation heavily inspired by https://observablehq.com/@dgreensp/implementing-fractional-indexing
 * This implementation uses a loop instead of recursion, which should perform better (haven't tested though ...)
 * 
 * Main idea is to represent fractions as strings. So f.x the fraction 0.5 gets the value "5", 0.75 gets "75" and so on
 * Because we use strings we preserve simple ordering by comparing strings a < b in lexicographical order
 * 
 * Base 62 is used for compressing the length and the final order is easy to understand. Figma f.x uses the full ascii range with base 92
 * 
 */

const BASE_62_DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
const BASE_10_DIGITS = "0123456789";

const DIGITS = BASE_62_DIGITS;
const VALUES: { [char: string]: number } = {};
[...DIGITS].forEach((c, i) => { VALUES[c] = i });

// a = "[" denotes start of list
// b = "]" denotes end of list
export const fracMid = (a: string, b: string) => {
    assert(a === "[" || a < b, "a >= b");

    if (a === "[" && b === "]") return DIGITS[Math.round(DIGITS.length / 2)];
    if (b === "]") {
        const max = DIGITS[DIGITS.length - 1];
        const maxValue = DIGITS.length;
        let i = 0;
        while (a.charAt(i) === max) i++;
        const maxes = padLeft('', max, i);
        const aValue = VALUES[a.charAt(i)];
        if (i === a.length) {
            // We consumed all max digits, put in the final mid
            const mid = Math.round(maxValue / 2);
            return maxes + mid;
        } else {
            // The end is some other value. Mid it with the max value
            const mid = Math.round((aValue + maxValue) / 2);
            return maxes + mid;
        }
    }
    if (a === "[") {
        const zero = DIGITS[0];
        let i = 0;
        while (b.charAt(i) === zero) i++;
        const bValue = VALUES[b.charAt(i)];
        const zeros = padLeft('', zero, i);
        if (bValue > 1) {
            const mid = Math.round(bValue / 2);
            return zeros + DIGITS[mid];
        } else {
            // No leading 1
            const maxValue = DIGITS.length;
            if (i === b.length - 1) {
                const mid = Math.round(maxValue / 2);
                return zeros + zero + DIGITS[mid];
            }

            // Leading 1's - Keep putting ones
            const one = DIGITS[1];
            let ones = "";
            let k = i + 1;
            while (b.charAt(k) === one) {
                ones = ones + one;
                k += 1;
            }
            const mid = Math.round(maxValue / 2);
            return ones + zero + DIGITS[mid];
        }
    }

    // Pad to the same length
    let A = a;
    let B = b;
    if (a.length < b.length) A = padRight(A, DIGITS[0], b.length - a.length);
    else if (a.length > b.length) B = padRight(B, DIGITS[0], a.length - b.length);

    // Find the longest common prefix
    let n = 0;
    while (A.charAt(n) === B.charAt(n)) n++;
    const lcp = A.slice(0, n);

    const aValue = VALUES[A.charAt(n)];
    const bValue = VALUES[B.charAt(n)];

    if (bValue - aValue > 1) {
        // We can squeeze inbetween. e.g 101, 103 -> 102
        const midDigit = Math.round((aValue + bValue) / 2);
        return lcp + DIGITS[midDigit];
    } else {
        // We can't squeeze inbetween. e.g 05, 1
        const maxValue = DIGITS.length;
        let nextAValue = VALUES[A.charAt(n + 1)];

        if (nextAValue === maxValue - 1) {
            // We can't squeeze in between because 09, 1
            // Need to keep putting down max until the end is not max anymore
            // e.g 0999, 1 -> 09995
            let i = n + 1;
            while (nextAValue === maxValue - 1 && i !== A.length) {
                nextAValue = VALUES[A.charAt(i)];
                i += 1;
            }
            const maxes = padLeft('', DIGITS[DIGITS.length - 1], i - 1);

            if (nextAValue === maxValue - 1) {
                // a ends on the max. Insert the mid number in the end
                // e.g 0999, 1 -> 09995
                const mid = DIGITS[Math.round(DIGITS.length / 2)];
                return lcp + DIGITS[aValue] + maxes + mid;
            } else {
                // a ends on something we can squeeze a number in between. Take the mid of that value with the max value
                // e.g 0998, 1 -> 0999
                const mid = Math.round((nextAValue + maxValue) / 2);
                return lcp + DIGITS[aValue] + maxes.slice(0, -1) + mid;
            }
        } else {
            if (n === A.length - 1) {
                // End of input. Insert half value at end
                const mid = Math.round(maxValue / 2);
                return lcp + DIGITS[aValue] + DIGITS[mid];
            } else {
                // We can squeeze in between. e.g 05, 1 -> 08
                const mid = Math.round((nextAValue + maxValue) / 2);
                return lcp + DIGITS[aValue] + DIGITS[mid];
            }
        }
    }
}

const padLeft = (str: string, char: string, n: number) => {
    return Array.from({ length: n }).map(() => char).join('') + str;
}

const padRight = (str: string, char: string, n: number) => {
    return str + Array.from({ length: n }).map(() => char).join('');
}