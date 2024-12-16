import { assert } from "jsr:@std/assert@^0.217.0/assert";

/**
 * Implementation heavily inspired by https://observablehq.com/@dgreensp/implementing-fractional-indexing
 * This implementation uses a loop instead of recursion, which should perform better (haven't tested though ...)
 * 
 * Main idea is to represent fractions as strings. So f.x the fraction 0.5 gets the value "5", 0.75 gets "75" and so on.
 * Because we use strings we preserve simple ordering by comparing strings a < b in lexicographical order
 * 
 * Base 62 is used for compressing the length but any ascii base could be used. Figma f.x uses the full ascii range with base 92
 * 
 */

export const BASE_10_DIGITS = "0123456789";
export const BASE_62_DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/**
 * @param a First fractional index anchor, "[" denotes start of list
 * @param b Second fractional index anchor, "]" denotes end of list
 * @param digits Any base encoding. Defaults to Base62
 * @returns Fractional index between a and b
 */
export const fracMid = (a: string, b: string, digits = BASE_62_DIGITS) => {
    assert(a === "[" || b === "]" || a < b, `${a} >= ${b}`);

    if (a === "[" && b === "]") return digits[Math.round(digits.length / 2)];
    if (b === "]") {
        const max = digits[digits.length - 1];
        const maxValue = digits.length;
        let i = 0;
        while (a.charAt(i) === max) i++;
        const maxes = padLeft('', max, i);
        const aValue = digits.indexOf(a.charAt(i))
        if (i === a.length) {
            // We consumed all max digits, put in the final mid
            const mid = Math.round(maxValue / 2);
            return maxes + mid;
        } else {
            // The end is some other value. Mid it with the max value
            const mid = Math.round((aValue + maxValue) / 2);
            return maxes + digits[mid];
        }
    }
    if (a === "[") {
        const zero = digits[0];
        let i = 0;
        while (b.charAt(i) === zero) i++;
        const bValue = digits.indexOf(b.charAt(i));
        const zeros = padLeft('', zero, i);
        if (bValue > 1) {
            const mid = Math.round(bValue / 2);
            return zeros + digits[mid];
        } else {
            // No leading 1
            const maxValue = digits.length;
            if (i === b.length - 1) {
                const mid = Math.round(maxValue / 2);
                return zeros + zero + digits[mid];
            }

            // Leading 1's - Keep putting ones
            const one = digits[1];
            let ones = "";
            let k = i + 1;
            while (b.charAt(k) === one) {
                ones = ones + one;
                k += 1;
            }
            const mid = Math.round(maxValue / 2);
            return ones + zero + digits[mid];
        }
    }

    // Pad to the same length
    let A = a;
    let B = b;
    if (a.length < b.length) A = padRight(A, digits[0], b.length - a.length);
    else if (a.length > b.length) B = padRight(B, digits[0], a.length - b.length);

    // Find the longest common prefix
    let n = 0;
    while (A.charAt(n) === B.charAt(n)) n++;
    const lcp = A.slice(0, n);

    const aValue = digits.indexOf(A.charAt(n));
    const bValue = digits.indexOf(B.charAt(n));

    if (bValue - aValue > 1) {
        // We can squeeze inbetween. e.g 101, 103 -> 102
        const midDigit = Math.round((aValue + bValue) / 2);
        return lcp + digits[midDigit];
    } else {
        // We can't squeeze inbetween. e.g 05, 1
        const maxValue = digits.length;
        let nextAValue = digits.indexOf(A.charAt(n + 1));

        if (nextAValue === maxValue - 1) {
            // We can't squeeze in between because 09, 1
            // Need to keep putting down max until the end is not max anymore
            // e.g 0999, 1 -> 09995
            let i = n + 1;
            while (nextAValue === maxValue - 1 && i !== A.length) {
                nextAValue = digits.indexOf(A.charAt(i));
                i += 1;
            }
            const maxes = padLeft('', digits[digits.length - 1], i - 1);

            if (nextAValue === maxValue - 1) {
                // a ends on the max. Insert the mid number in the end
                // e.g 0999, 1 -> 09995
                const mid = digits[Math.round(digits.length / 2)];
                return lcp + digits[aValue] + maxes + mid;
            } else {
                // a ends on something we can squeeze a number in between. Take the mid of that value with the max value
                // e.g 0998, 1 -> 0999
                const mid = Math.round((nextAValue + maxValue) / 2);
                return lcp + digits[aValue] + maxes.slice(0, -1) + mid;
            }
        } else {
            if (n === A.length - 1) {
                // End of input. Insert half value at end
                const mid = Math.round(maxValue / 2);
                return lcp + digits[aValue] + digits[mid];
            } else {
                // We can squeeze in between. e.g 05, 1 -> 08
                const mid = Math.round((nextAValue + maxValue) / 2);
                return lcp + digits[aValue] + digits[mid];
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