/**
 * An implementation of hybrid-logical-clock as described in:
 * https://cse.buffalo.edu/tech-reports/2014-04.pdf
 */

export type Hlc = {
    pt: number  // Physical-time (Wall clock reading)
    lt: number  // Logical-time (Lamport timestamp)
}

export const newHlc = (): Hlc => {
    return {
        pt: (new Date).getTime(),
        lt: 0,
    }
}

export const newEncodedHlc = (): string => {
    const hlc = newHlc();
    return encodeHlc(hlc);
}

export const sendHlc = (clock: Hlc): Hlc => {
    let lt = clock.lt;
    let pt = (new Date).getTime();
    if (pt <= clock.pt) {
        pt = clock.pt;
        lt += 1;
    } else {
        lt = 0;
    }

    return { pt, lt };
}

export const receiveHlc = (clock: Hlc, m: Hlc): Hlc => {
    const pt = (new Date).getTime();

    const lOld = clock.pt;

    let cNew = clock.lt;
    const lNew = Math.max(lOld, m.pt, pt);
    if (lNew === lOld && lNew === m.pt) {
        // Our largest seen physical-time is equal to theirs. Pick next highest logical time between us and them
        cNew = Math.max(cNew, m.lt) + 1;
    } else if (lNew === lOld) {
        // Our largest seen pt is higher than theirs but our physical clock is still behind
        cNew = cNew + 1;
    } else if (lNew === m.pt) {
        // Their physical clock is higher than ours
        cNew = m.lt + 1;
    } else {
        // Our physical clock is higher than theirs, reset the logical clock part
        cNew = 0;
    }

    return { pt: lNew, lt: cNew };
}

export const encodeHlc = (clock: Hlc): string => {
    return `${clock.pt.toString(36)}-${clock.lt.toString(36)}`;
}

export const decodeHlc = (encoded: string): Hlc => {
    const [pt36, lt36] = encoded.split("-");
    if (!pt36 || !lt36) {
        throw new Error(`Failed to decode hybrid-logical-clock: '${encoded}'`);
    }

    const pt = Number.parseInt(pt36, 36);
    const lt = Number.parseInt(lt36, 36);

    return { pt, lt };
}