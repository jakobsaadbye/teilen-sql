import { base57mid } from "./base57.ts";
import { base57decode, base57encode } from "./base57.ts";

const main = () => {
    // const strs = [];
    // const step = 0.001;
    // let x = 0;
    // while (x <= 0.1) {
    //     const str = base57encode(x);
    //     console.log(`${x} -> '${str}'`);
    //     x += step;
    //     strs.push(str);
    // }

    const a = base57encode(0.25);
    const b = base57encode(0.75);
    const c = base57mid(a, b);

    console.log(c, base57decode(c));
    
    // for (const str of strs) {
    //     const x = base57decode(str);
    //     console.log(`${str} -> '${x}'`);
    // }
}

main();