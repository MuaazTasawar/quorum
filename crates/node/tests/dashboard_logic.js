// Tests for the dashboard's pure logic. Loads the REAL crates/node/src/dashboard.html,
// extracts its inline <script>, and runs it in a vm sandbox with no DOM, so what is
// tested is the shipped code, not a copy.
//
//   node crates/node/tests/dashboard_logic.js
"use strict";
const fs = require("fs");
const path = require("path");
const vm = require("vm");
const assert = require("assert");

const html = fs.readFileSync(path.join(__dirname, "..", "src", "dashboard.html"), "utf8");
const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)];
assert.strictEqual(scripts.length, 1, "exactly one inline script");
const sandbox = { window: {}, location: { host: "x", href: "http://x/dashboard" }, setTimeout, clearTimeout };
vm.createContext(sandbox);
vm.runInContext(scripts[0][1], sandbox);
const D = sandbox.window.QuorumDashboard;
assert.ok(D, "the page exposes its pure functions");

let passed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log("ok   " + name); }
  catch (e) { console.log("FAIL " + name + "\n     " + e.message); process.exitCode = 1; }
}
const eq = (a, b) => assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b);

const snap = (id, role, term, extra) => Object.assign(
  { node_id: id, role: role, current_term: term, members: [1, 2, 3], commit_index: 10,
    last_applied: 10, snapshots_installed: 0 }, extra || {});
const up = (id, role, term, extra) => ({ addr: "n" + id + ":80", ok: true, snap: snap(id, role, term, extra) });
const down = (id) => ({ addr: "n" + id + ":80", ok: false, snap: null });

// ---- parseNodes ----
test("parseNodes: splits on commas and whitespace, keeps order", () =>
  eq(D.parseNodes("a:1, b:2  c:3,d"), ["a:1", "b:2", "c:3", "d"]));
test("parseNodes: drops duplicates", () => eq(D.parseNodes("a:1,a:1,b:2,a:1"), ["a:1", "b:2"]));
test("parseNodes: null/undefined/empty", () => {
  eq(D.parseNodes(null), []); eq(D.parseNodes(undefined), []); eq(D.parseNodes(""), []);
});
test("parseNodes: rejects anything that could change the URL", () => {
  for (const bad of ["evil.com/path", "a:1/x", "http://a:1", "a:1?x=1", "a:1#f", "user@host:1",
                     "a:1x", "a:123456", "a:", ":80", "<script>", "a:1;b", "../x", "a%20b",
                     "a\\b", "[::1]:80"]) {
    eq(D.parseNodes(bad), []);
  }
});
test("parseNodes: keeps good entries next to bad ones", () =>
  eq(D.parseNodes("good:1,http://bad,also-good.example:8080"), ["good:1", "also-good.example:8080"]));
test("parseNodes: accepts a bare hostname and IPv4", () => eq(D.parseNodes("localhost,127.0.0.1:9"), ["localhost", "127.0.0.1:9"]));

// ---- nodeUrl ----
test("nodeUrl: own host is asked relatively, others absolutely", () => {
  assert.strictEqual(D.nodeUrl("h:1", "h:1"), "/health");
  assert.strictEqual(D.nodeUrl("o:2", "h:1"), "http://o:2/health");
});

// ---- commitRate ----
test("commitRate: unknown with fewer than two points", () => {
  assert.strictEqual(D.commitRate(null), null);
  assert.strictEqual(D.commitRate([]), null);
  assert.strictEqual(D.commitRate([{ t: 0, commit: 1 }]), null);
});
test("commitRate: plain rate", () =>
  assert.strictEqual(D.commitRate([{ t: 0, commit: 0 }, { t: 1000, commit: 50 }]), 50));
test("commitRate: only the last 5 seconds count", () => {
  const h = [{ t: 0, commit: 0 }, { t: 1000, commit: 1000 }, { t: 10000, commit: 1000 }, { t: 12000, commit: 1100 }];
  // window: points within 5s of t=12000 -> t=10000 and t=12000 -> 100 commits / 2 s
  assert.strictEqual(D.commitRate(h), 50);
});
test("commitRate: never negative after a restart resets the counter", () =>
  assert.strictEqual(D.commitRate([{ t: 0, commit: 100 }, { t: 1000, commit: 5 }]), 0));
test("commitRate: identical timestamps give null, not Infinity/NaN", () =>
  assert.strictEqual(D.commitRate([{ t: 5, commit: 1 }, { t: 5, commit: 9 }]), null));

// ---- summarize ----
test("summarize: healthy", () => {
  const r = D.summarize([up(1, "Leader", 3), up(2, "Follower", 3), up(3, "Follower", 3)]);
  assert.strictEqual(r.status, "healthy");
  eq(r.leaders, [1]); assert.strictEqual(r.term, 3); assert.strictEqual(r.quorum, 2);
  assert.match(r.headline, /Leader: node 1 \(term 3\), all 3 nodes reachable/);
});
test("summarize: nothing reachable", () => {
  assert.strictEqual(D.summarize([down(1), down(2), down(3)]).status, "unreachable");
  assert.strictEqual(D.summarize([]).status, "unreachable");
});
test("summarize: a node that answered with ok but no snapshot counts as down", () =>
  assert.strictEqual(D.summarize([{ addr: "a", ok: true, snap: null }]).status, "unreachable"));
test("summarize: no leader", () => {
  const r = D.summarize([up(1, "Candidate", 4), up(2, "Follower", 4), up(3, "Follower", 3)]);
  assert.strictEqual(r.status, "no-leader"); assert.match(r.headline, /term 4/);
});
test("summarize: degraded when a node is down but quorum holds", () => {
  const r = D.summarize([up(1, "Leader", 3), up(2, "Follower", 3), down(3)]);
  assert.strictEqual(r.status, "degraded"); assert.match(r.headline, /2 of 3/);
});
test("summarize: no-quorum when reachable nodes are fewer than a majority", () => {
  const r = D.summarize([up(1, "Leader", 3), down(2), down(3)]);
  assert.strictEqual(r.status, "no-quorum"); assert.match(r.headline, /1 of 2/);
});
test("summarize: two leaders in the SAME term is an error", () => {
  const r = D.summarize([up(1, "Leader", 3), up(2, "Leader", 3), up(3, "Follower", 3)]);
  assert.strictEqual(r.status, "error");
  assert.ok(r.notes.some((n) => /TWO LEADERS IN TERM 3/.test(n)));
});
test("summarize: two leaders in DIFFERENT terms is not an error, the stale one is named", () => {
  const r = D.summarize([up(1, "Leader", 2), up(2, "Leader", 3), up(3, "Follower", 3)]);
  assert.notStrictEqual(r.status, "error");
  assert.ok(r.notes.some((n) => /node 1 still thinks it leads/.test(n)), JSON.stringify(r.notes));
  assert.ok(!r.notes.some((n) => /node 2 still/.test(n)));
});
test("summarize: a LONE leader whose term is behind the cluster is not reported as healthy", () => {
  // node 1 is a stale leader (term 3) while 2 and 3 already moved to term 4.
  const r = D.summarize([up(1, "Leader", 3), up(2, "Follower", 4), up(3, "Candidate", 4)]);
  assert.notStrictEqual(r.status, "healthy", r.headline);
});
test("summarize: quorum follows the leader's member count, not the page's node list", () => {
  // 5 members but the page only watches 3 of them, all up.
  const m = [1, 2, 3, 4, 5];
  const r = D.summarize([up(1, "Leader", 3, { members: m }), up(2, "Follower", 3, { members: m }),
                         up(3, "Follower", 3, { members: m })]);
  assert.strictEqual(r.quorum, 3); assert.strictEqual(r.status, "healthy");
  const r2 = D.summarize([up(1, "Leader", 3, { members: m }), up(2, "Follower", 3, { members: m })]);
  assert.strictEqual(r2.status, "no-quorum");
});
test("summarize: tolerates a snapshot without members", () => {
  const s = up(1, "Leader", 1); delete s.snap.members;
  assert.doesNotThrow(() => D.summarize([s]));
});

// ---- diffEvents ----
const state = (list) => { const o = {}; list.forEach((s) => { o[s.addr] = { ok: s.ok, snap: s.snap }; }); return o; };
test("diffEvents: nothing changed -> no events", () => {
  const s = state([up(1, "Leader", 3), up(2, "Follower", 3)]);
  eq(D.diffEvents(s, s), []);
});
test("diffEvents: first sight of a node produces no events", () =>
  eq(D.diffEvents({}, state([up(1, "Leader", 3)])), []));
test("diffEvents: unreachable and reachable again", () => {
  const a = state([up(1, "Leader", 3)]), b = state([down(1)]);
  const ev = D.diffEvents(a, b);
  assert.strictEqual(ev.length, 1); assert.match(ev[0], /node 1 .* became unreachable/);
  const back = D.diffEvents(b, a);
  assert.strictEqual(back.length, 1); assert.match(back[0], /reachable again/);
});
test("diffEvents: role change mentions both roles and the term", () => {
  const ev = D.diffEvents(state([up(1, "Leader", 3)]), state([up(1, "Follower", 4)]));
  assert.strictEqual(ev.length, 1); assert.match(ev[0], /Leader.*Follower.*term 4/);
});
test("diffEvents: term-only change", () => {
  const ev = D.diffEvents(state([up(1, "Follower", 3)]), state([up(1, "Follower", 4)]));
  eq(ev, ["node 1 moved to term 4"]);
});
test("diffEvents: membership change and snapshot install", () => {
  const a = state([up(1, "Follower", 3)]);
  const b = state([up(1, "Follower", 3, { members: [1, 2, 3, 4], snapshots_installed: 1 })]);
  const ev = D.diffEvents(a, b);
  assert.ok(ev.some((e) => /members are now \[1,2,3,4\]/.test(e)), ev.join("|"));
  assert.ok(ev.some((e) => /installed a snapshot/.test(e)), ev.join("|"));
});

// ---- the shipped page itself ----
test("page: no external loads and DOM written via textContent/createTextNode only", () => {
  assert.ok(!/\b(src|href)=["']?https?:/i.test(html));
  assert.ok(!/innerHTML|outerHTML|insertAdjacentHTML|document\.write|eval\(/.test(html),
    "no HTML-injecting API may be used");
});

console.log("\n" + passed + " passed" + (process.exitCode ? ", SOME FAILED" : ""));
