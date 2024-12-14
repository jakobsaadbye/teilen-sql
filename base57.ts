/**
 * OLD Implementation NOT USED.
 * Fell short as its not arbitrary precision for fractions
 */

const CHAR_SET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz";
const CHAR_TO_VALUE: { [char: string]: number } = {};
[...CHAR_SET].forEach((c, i) => { CHAR_TO_VALUE[c] = i });


export const base57encode = (x: number): string => {
    if (x <= 0) return 'A';
    if (x >= 1) return 'z';

    const N = 3; // scale
    const scaled = Math.floor(x * Math.pow(57, N));

    const xs = []; // raw digits
    let rem;
    let s = scaled;
    while (s > 0) {
        rem = s % 57;
        s = Math.floor(s / 57);
        xs.unshift(rem);
    }
    while (xs.length !== N) {
        xs.unshift(0);
    }

    const ascii = xs.map(x => x + 65);
    return String.fromCharCode(...ascii);
}

export const base57decode = (encoded: string) : number => {
    if (encoded === 'A') return 0;
    if (encoded === 'z') return 1;

    const N = 3;
    const scaled = scalarFromBase57(encoded);

    const fraction = scaled / Math.pow(57, N);
    return fraction;
}

export const base57mid = (encodedA: string, encodedB: string) => {
    const scalarA = scalarFromBase57(encodedA);
    const scalarB = scalarFromBase57(encodedB);
    const mid = (scalarA + scalarB) / 2;
    return scalarToBase57(mid);
}

const scalarToBase57 = (scalar: number) => {
    const xs = []; // raw digits
    let rem;
    let s = scalar;
    while (s !== 0) {
        rem = s % 57;
        s = Math.floor(s / 57);
        xs.push(rem);
    }

    const ascii = xs.map(x => x + 65);
    return String.fromCharCode(...ascii);
}

const scalarFromBase57 = (encoded: string) => {
    const digits = base57raw(encoded);
    let scaled = 0;
    for (let i = digits.length - 1; i >= 0; i--) {
        scaled = scaled * 57 + digits[i];
    }
    return scaled;
}

const base57raw = (encoded: string): number[] => {
    return [...encoded].map(c => CHAR_TO_VALUE[c]);
}