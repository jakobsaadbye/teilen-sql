import * as React from "react"
import { SVGProps } from "react"
const SvgComponent = (props: SVGProps<SVGSVGElement>) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    width={24}
    height={24}
    fill="#e8eaed"
    viewBox="0 -960 960 960"
    {...props}
  >
    <path d="M400-280v-400l200 200-200 200Z"/>
  </svg>
)
export default SvgComponent
