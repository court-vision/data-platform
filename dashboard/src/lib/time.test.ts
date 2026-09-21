import { describe, expect, test } from "bun:test"

import { formatCentral, formatDuration, parseUtc, relativeTime } from "@/lib/time"

describe("parseUtc", () => {
  test("reads an offset-less timestamp as UTC, not local time", () => {
    expect(parseUtc("2026-09-21T07:04:11")?.toISOString()).toBe("2026-09-21T07:04:11.000Z")
  })

  test("leaves an explicit zone alone", () => {
    expect(parseUtc("2026-09-21T07:04:11Z")?.toISOString()).toBe("2026-09-21T07:04:11.000Z")
    expect(parseUtc("2026-09-21T02:04:11-05:00")?.toISOString()).toBe("2026-09-21T07:04:11.000Z")
  })

  test("null, empty and garbage are null", () => {
    expect(parseUtc(null)).toBeNull()
    expect(parseUtc("")).toBeNull()
    expect(parseUtc("not a date")).toBeNull()
  })
})

describe("relativeTime", () => {
  const now = Date.parse("2026-09-21T12:00:00Z")

  test.each([
    ["2026-09-21T11:59:58", "just now"],
    ["2026-09-21T11:59:30", "30s ago"],
    ["2026-09-21T11:15:00", "45m ago"],
    ["2026-09-21T07:00:00", "5h ago"],
    ["2026-09-18T12:00:00", "3d ago"],
  ])("%s -> %s", (iso, expected) => {
    expect(relativeTime(iso, now)).toBe(expected)
  })

  test("a missing timestamp is a dash", () => {
    expect(relativeTime(null, now)).toBe("—")
  })
})

describe("formatDuration", () => {
  test.each([
    [null, "—"],
    [0.25, "250ms"],
    [12.34, "12.3s"],
    [61, "1m 1s"],
  ])("%p -> %s", (seconds, expected) => {
    expect(formatDuration(seconds)).toBe(expected)
  })
})

test("formatCentral renders in Central whatever the machine's zone", () => {
  expect(formatCentral("2026-09-21T07:04:00")).toBe("Sep 21, 2:04 AM CT")
})
