import { Hono } from 'hono'

const app = new Hono()

// ANSI color codes
const c = {
  reset:   '\x1b[0m',
  bold:    '\x1b[1m',
  dim:     '\x1b[2m',
  red:     '\x1b[31m',
  green:   '\x1b[32m',
  yellow:  '\x1b[33m',
  blue:    '\x1b[34m',
  cyan:    '\x1b[36m',
  white:   '\x1b[37m',
  grey:    '\x1b[90m',
}

const LEVEL_COLOR: Record<string, string> = {
  ERROR:   c.red,
  PANIC:   c.red,
  FATAL:   c.red,
  WARNING: c.yellow,
  NOTICE:  c.cyan,
  INFO:    c.green,
  LOG:     c.white,
  DEBUG:   c.grey,
  DEBUG1:  c.grey,
  DEBUG2:  c.grey,
  DEBUG3:  c.grey,
  DEBUG4:  c.grey,
  DEBUG5:  c.grey,
}

interface LogEvent {
  timestamp: string
  level: string
  message: string
  detail: string | null
  hint: string | null
  context: string | null
  database: string | null
  user: string | null
  query: string | null
  sqlerrcode: number
  filename: string | null
  lineno: number | null
  funcname: string | null
}

function formatTime(iso: string): string {
  // e.g. "2026-03-12T14:00:28.663350174+00:00" → "14:00:28.663"
  const match = iso.match(/T(\d{2}:\d{2}:\d{2})\.(\d{3})/)
  if (match) return `${match[1]}.${match[2]}`
  return iso
}

function renderEvent(ev: LogEvent): string {
  const color = LEVEL_COLOR[ev.level] ?? c.white
  const time  = formatTime(ev.timestamp)
  const level = ev.level.padEnd(7)

  let line = `${color}[${time}] ${c.bold}${level}${c.reset}${color}  ${ev.message}`

  const extras: string[] = []
  if (ev.database) extras.push(`db=${ev.database}`)
  if (ev.user)     extras.push(`user=${ev.user}`)
  if (ev.query)    extras.push(`query=${ev.query.trim().replace(/\s+/g, ' ')}`)
  if (ev.detail)   extras.push(`detail=${ev.detail}`)
  if (ev.hint)     extras.push(`hint=${ev.hint}`)

  if (extras.length) {
    line += `  ${c.dim}(${extras.join(', ')})${c.reset}${color}`
  }

  line += c.reset
  return line
}

function renderBatch(method: string, url: string, body: string): void {
  console.log(`${c.bold}${c.blue}→ ${method} ${url}${c.reset}`)

  let events: LogEvent[]
  try {
    const parsed = JSON.parse(body)
    events = Array.isArray(parsed) ? parsed : [parsed]
  } catch {
    // Not JSON — just print raw
    console.log(`${c.grey}${body}${c.reset}`)
    console.log(`${c.dim}${'─'.repeat(72)}${c.reset}`)
    return
  }

  for (const ev of events) {
    console.log(renderEvent(ev))
  }

  console.log(`${c.dim}${'─'.repeat(72)}${c.reset}`)
}

app.get('/ping', (c) => c.text('pong'))

app.all('*', async (ctx) => {
  const method = ctx.req.method
  const url    = ctx.req.url
  const body   = await ctx.req.text()

  if (url.endsWith('/ping')) {
    return ctx.text('pong')
  }

  renderBatch(method, url, body)

  return ctx.text('OK')
})

console.log(`${c.bold}${c.green}Mock server started on port 8080${c.reset}`)

export default {
  port: 8080,
  fetch: app.fetch,
}
