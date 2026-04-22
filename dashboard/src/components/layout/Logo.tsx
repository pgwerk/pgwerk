interface LogoProps {
  theme: 'light' | 'dark'
  size?: number
}

export function Logo({ theme, size = 28 }: LogoProps) {
  if (theme === 'dark') {
    return (
      <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 44 44" width={size} height={size} fill="none">
        <rect x="1" y="1" width="42" height="42" rx="8" fill="#F8FAFC"/>
        <text
          x="22" y="29"
          fontFamily="'JetBrains Mono', 'Fira Code', 'SF Mono', 'Consolas', monospace"
          fontSize="16" fontWeight="600" fill="#0F172A"
          textAnchor="middle" letterSpacing="0.5"
        >wrk</text>
      </svg>
    )
  }

  return (
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 44 44" width={size} height={size} fill="none">
      <rect x="1" y="1" width="42" height="42" rx="8" fill="#0F172A"/>
      <text
        x="22" y="29"
        fontFamily="'JetBrains Mono', 'Fira Code', 'SF Mono', 'Consolas', monospace"
        fontSize="16" fontWeight="600" fill="#F8FAFC"
        textAnchor="middle" letterSpacing="0.5"
      >wrk</text>
    </svg>
  )
}
