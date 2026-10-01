import { describe, expect, test } from "bun:test"

import { daysBetween, formatCentral, formatDay, formatDuration, parseUtc, relativeTime } from "@/lib/time"

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

import { formatCentralLong, formatClockCentral, formatUptime } from "@/lib/time"

test("formatClockCentral is a Central wall-clock time", () => {
  expect(formatClockCentral(Date.parse("2026-09-24T18:00:00Z"))).toBe("1:00 PM")
})

test("formatCentralLong carries seconds", () => {
  expect(formatCentralLong("2026-09-24T18:00:07")).toBe("Sep 24, 1:00:07 PM CT")
})

describe("formatUptime", () => {
  test.each([
    [null, "—"],
    [40, "40s"],
    [300, "5m"],
    [7_800, "2h 10m"],
    [273_600, "3d 4h"],
  ])("%p -> %s", (seconds, expected) => {
    expect(formatUptime(seconds)).toBe(expected)
  })
})

describe("formatDay", () => {
  test("a plain date never shifts with the zone", () => {
    expect(formatDay("2026-03-04")).toBe("Mar 4")
    expect(formatDay("2026-10-21")).toBe("Oct 21")
  })

  test("missing or malformed is a dash", () => {
    expect(formatDay(null)).toBe("—")
    expect(formatDay("not a date")).toBe("—")
  })
})

describe("daysBetween", () => {
  test("whole days, signed", () => {
    expect(daysBetween("2026-03-02", "2026-03-04")).toBe(2)
    expect(daysBetween("2026-03-04", "2026-03-04")).toBe(0)
    expect(daysBetween("2026-03-04", "2026-03-01")).toBe(-3)
  })
})
