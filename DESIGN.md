---
name: YAIF Proof Sheet
description: A Risograph proof sheet for a Databricks ingestion reference — three spot inks on warm stock, with real overprints.
colors:
  paper: "#FFF6E8"
  blue: "#1E4FFF"
  pink: "#FF3EA5"
  sun: "#FFD22A"
  violet: "#6A3DFF"
  tangerine: "#FF8A3C"
  leaf: "#23B36B"
  ink: "#101340"
  ink-mid: "#454a80"
  code-comment: "#4a4f78"
  code-string: "#12734a"
  code-keyword: "#ae1866"
typography:
  display:
    fontFamily: "Archivo Black, Archivo, Helvetica Neue, Helvetica, Arial, sans-serif"
    fontSize: "clamp(2.5rem, 7.4vw, 6rem)"
    fontWeight: 400
    lineHeight: 0.87
    letterSpacing: "-0.028em"
  headline:
    fontFamily: "Archivo Black, Archivo, Helvetica Neue, Helvetica, Arial, sans-serif"
    fontSize: "clamp(1.75rem, 3.4vw, 3rem)"
    fontWeight: 400
    lineHeight: 0.87
    letterSpacing: "-0.028em"
  deck:
    fontFamily: "Archivo, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif"
    fontSize: "clamp(1.0625rem, 1.15vw + 0.8rem, 1.3125rem)"
    fontWeight: 500
    lineHeight: 1.42
  title:
    fontFamily: "Archivo, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif"
    fontSize: "1.0625rem"
    fontWeight: 700
    lineHeight: 1.25
    letterSpacing: "-0.01em"
  body:
    fontFamily: "Archivo, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif"
    fontSize: "clamp(0.975rem, 0.32vw + 0.9rem, 1.0625rem)"
    fontWeight: 400
    lineHeight: 1.55
  label:
    fontFamily: "Archivo, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif"
    fontSize: "0.6875rem"
    fontWeight: 700
    lineHeight: 1.3
    letterSpacing: "0.11em"
  mono:
    fontFamily: "Spline Sans Mono, ui-monospace, SFMono-Regular, Menlo, monospace"
    fontSize: "0.8125rem"
    fontWeight: 400
    lineHeight: 1.65
  wordmark:
    fontFamily: "Archivo Black, Archivo, Helvetica Neue, Helvetica, Arial, sans-serif"
    fontSize: "clamp(1.9rem, 3.4vw, 2.6rem)"
    fontWeight: 400
    lineHeight: 0.87
    letterSpacing: "-0.028em"
  subhead:
    fontFamily: "Archivo Black, Archivo, Helvetica Neue, Helvetica, Arial, sans-serif"
    fontSize: "clamp(1.15rem, 2vw, 1.6rem)"
    fontWeight: 400
    lineHeight: 0.87
    letterSpacing: "-0.028em"
  item:
    fontFamily: "Archivo, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif"
    fontSize: "0.9375rem"
    fontWeight: 700
    lineHeight: 1.25
    letterSpacing: "-0.01em"
  small:
    fontFamily: "Archivo, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif"
    fontSize: "0.875rem"
    fontWeight: 400
    lineHeight: 1.55
  fine:
    fontFamily: "Archivo, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif"
    fontSize: "0.75rem"
    fontWeight: 400
    lineHeight: 1.35
  micro:
    fontFamily: "Archivo, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif"
    fontSize: "0.625rem"
    fontWeight: 700
    lineHeight: 1.25
    letterSpacing: "0.02em"
  plate:
    fontFamily: "Archivo Black, Archivo, Helvetica Neue, Helvetica, Arial, sans-serif"
    fontSize: "clamp(1.5rem, 2.6vw, 2.1rem)"
    fontWeight: 400
    lineHeight: 0.87
    letterSpacing: "-0.028em"
rounded:
  none: "0"
  disc: "50%"
spacing:
  gutter: "clamp(1.25rem, 3.2vw, 2.75rem)"
  band-y: "clamp(2.75rem, 5.5vw, 4.75rem)"
  band-hd: "clamp(1.5rem, 3vw, 2.5rem)"
  hero-gap: "clamp(1.5rem, 2.6vw, 2.75rem)"
components:
  button-primary:
    backgroundColor: "{colors.blue}"
    textColor: "{colors.paper}"
    typography: "{typography.label}"
    rounded: "{rounded.none}"
    padding: "0.85rem 1.35rem"
  button-primary-hover:
    backgroundColor: "{colors.blue}"
    textColor: "{colors.paper}"
  button-ghost:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.blue}"
    typography: "{typography.label}"
    rounded: "{rounded.none}"
    padding: "0.85rem 1.35rem"
  chip-stamp:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.blue}"
    typography: "{typography.label}"
    rounded: "{rounded.none}"
    padding: "0.3rem 0.55rem"
  chip-stamp-hot:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.pink}"
    padding: "0.3rem 0.55rem"
  state-ships:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.blue}"
    typography: "{typography.label}"
    padding: "0.1rem 0.35rem"
  state-setup:
    backgroundColor: "{colors.sun}"
    textColor: "{colors.ink}"
    typography: "{typography.label}"
    padding: "0.1rem 0.35rem"
  state-customer:
    backgroundColor: "{colors.pink}"
    textColor: "{colors.ink}"
    typography: "{typography.label}"
    padding: "0.1rem 0.35rem"
  field-sun:
    backgroundColor: "{colors.sun}"
    textColor: "{colors.ink}"
    padding: "0.8rem 1rem"
  field-blue:
    backgroundColor: "{colors.blue}"
    textColor: "{colors.paper}"
    padding: "1.1rem 1.15rem 1.25rem"
  field-pink:
    backgroundColor: "{colors.pink}"
    textColor: "{colors.ink}"
    padding: "0.85rem 1rem"
  plate-row:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    padding: "0.72rem 1rem"
  plate-row-hover:
    backgroundColor: "{colors.sun}"
    textColor: "{colors.ink}"
  code-block:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    typography: "{typography.mono}"
    rounded: "{rounded.none}"
    padding: "0.9rem 0.85rem 1rem"
---

# Design System: YAIF Proof Sheet

**Scope: this document governs the web surface only — the single static page at
`docs/index.html`.** It has no authority over the rest of the repository, which is a
Databricks Asset Bundle (YAML, Python, docs). Nothing here constrains `resources/`, `src/`,
the markdown guides that share the `docs/` directory, or `databricks.yml`; conversely, the
bundle's conventions do not constrain these tokens. If you are working on the bundle, close
this file.

The page sits in `docs/` because that is a native GitHub Pages source, not because it
belongs to the markdown guides beside it. `docs/.nojekyll` keeps Pages from processing the
directory.

Everything below was read out of the built page. Where the direction contract in that file's
`<body>` comment and the implementation disagree, the implementation is recorded and the
divergence is named.

## Overview

**Creative North Star: "The Press Proof Sheet"**

This is a Risograph proof sheet, not a poster about one. The page behaves like a sheet that
has been through a duplicator: warm uncoated stock (`#FFF6E8`), three spot inks laid down in
sequence, visible misregistration where a plate landed a few points off, mechanical grain in
every flooded area, and the press operator's own furniture — registration crosses at the
corners of the pull, a nine-cell colour bar at the foot. The metaphor is load-bearing rather
than decorative: five ingestion modules are drawn as five ink separations, and the places
where inks overprint are the places where code is genuinely shared. The direction contract
names the thesis plainly — a duplicator, not a framework; 900 endpoints become 45 masters.

Density is high and deliberately unpadded. Structure is asserted with 3px ink rules and
shared edges rather than with cards floating on whitespace: the medallion is three plates of
one run sharing borders, the comparison table is one bordered block, the connector chooser is
a single ruled column. Nothing is rounded, nothing is shadowed, nothing glows. The page is
loud in colour and quiet in geometry.

Two anti-references are stated in the contract and both hold in the build: it refuses the
dev-tool dark-hero-with-gradient page, and it refuses the print-studio marketing page this
visual world usually produces. The tell that it stayed on the right side of the second one is
that every ink decision is doing informational work — run states are ink densities, the
sibling SQL Server pattern is the same ink at screen, the Oracle EPM module is literally the
blue-over-pink overprint because it is the API module with a different auth mode.

**Key Characteristics:**

- Warm stock, never white; three spot inks plus three declared overprints.
- Zero border radius except one sunflower disc; depth comes from ink order, never shadow.
- Grain on every flooded ink area, achieved by letting paper break through — not by darkening.
- Archivo Black display with a real second pink ink layer offset 3–4px and multiplied.
- Mono reserved for code and measured values; never for a sentence.
- Cut-paper geometry is the entire icon language — no icon library anywhere on the page.
- One authored motion moment (the sheet taking ink), fully gated behind reduced-motion.

## Colors

Three saturated spot inks and their three pairwise overprints on a warm cream stock, with a
near-navy "blue at full density" carrying all long-form copy.

### Primary

- **Federal Blue** (`#1E4FFF`): structure and navigation. Every heavy rule, every border on a
  framed block, all link text, the bold spans inside the deck, the `.c-fn` function names in
  code, and the base layer of every display heading. It is also the one ink used as a field
  with paper-coloured type on top. This is the ink that says "this is the frame".
- **Fluoro Pink** (`#FF3EA5`): signal and action. The misregister layer under every display
  heading, the under-ink of the primary button, the focus ring, link hover, the `.c-kw`
  keywords in code, the drawn list-marker rules in the colophon, and the solid field behind
  the caution block and the customer-run-only state. Reserved for the things that want a
  reaction.

### Secondary

- **Sunflower** (`#FFD22A`): fields, plates and washes. The bronze plate, the chooser header,
  the run record, the table header row, the ghost button's under-ink, the hover wash on a
  chooser row, the screen behind the "needs external setup" state, and the cut disc behind
  the headline. **Sunflower never sets a type colour anywhere in the file** — it exists only
  as something ink sits on. (The brief for this document called it "fields only"; the code is
  slightly broader — it is also a plate, a screen, and an under-ink — but the constraint that
  matters holds: it is never a foreground.)

### Tertiary

The three declared overprints. These are what two inks *would* make if the drum ran twice.

- **Violet** (`#6A3DFF`, blue × pink): carries real meaning as well as decoration — it is the
  swatch for the Oracle EPM separation, because that module is the API module (blue) doing
  POST with Basic auth (pink). Also painted in the separation figure and the colour bar.
- **Leaf** (`#23B36B`, blue × sun): painted in the separation figure and the colour bar. Its
  darker run, `#12734a`, is the string colour in code blocks.
- **Tangerine** (`#FF8A3C`, pink × sun): painted in the separation figure and the colour bar
  and nowhere else. It is declared and displayed but carries **no semantic role** in the
  built page. Recorded as-is, not smoothed over: if a new surface needs a fourth signal, this
  is the unclaimed ink.

### Neutral

- **Warm Stock** (`#FFF6E8`): the page ground, and the paper that breaks through every grain
  mask. It is also the type colour on the blue field and inside the darkest region of the
  separation figure.
- **Blue at Full Density** (`#101340`): all long-form copy, all body prose, all code, and
  every heavy rule that frames an ink field rather than a blue-framed block. Reads as
  near-black but is unmistakably the blue family, which is why the page never feels like
  black-on-cream.
- **Lighter Run** (`#454a80`): secondary copy only — separation metadata, chooser subtitles,
  captions, code notes, the imprint.
- **Comment Grey-Violet** (`#4a4f78`): code comments. Deliberately close to the lighter run so
  comments recede without changing family.

### Named Rules

**The Painted Overprint Rule.** An overprint that the page *asserts* is painted at its
declared hex. An overlap that is merely *decorative* is multiplied. This is the single most
load-bearing decision in the system and the easiest to get wrong twice. The reason:
`mix-blend-mode: multiply` between two saturated spot inks can only travel darker, and every
declared overprint is lighter than at least one parent — multiplying `#1E4FFF` with `#FF3EA5`
can never arrive at `#6A3DFF`. So in the separation figure each intersection is drawn as its
own clipped region filled with the declared ink, and the colour bar paints all three
overprints as flat swatches. Multiply is used only where nobody is claiming a specific
result: the hero's cut plates over each other, the display misregister layer, the button
under-ink, the chooser hover wash, and the icons' second layer. **If you can name the colour
the overlap should be, paint it. If you can't, multiply it.**

**The Screen-Not-New-Ink Rule.** A relationship between two things gets expressed as the same
ink at reduced density, never as a fourth hue. SQL Server query-based is the pink separation
at screen because it is the SQL Server module's sibling pattern. Adding a new spot colour to
express a variant is how this system dies.

**The Sunflower-Never-Speaks Rule.** Sunflower is a surface, never a voice. No text, no icon
stroke, no rule is ever `#FFD22A`. Ink text sits on it at 12.2:1; the reverse never happens.

## Typography

**Display Font:** Archivo Black (falling back through Archivo → Helvetica Neue → Helvetica →
Arial)
**Body Font:** Archivo (falling back through the platform UI sans)
**Label/Mono Font:** Spline Sans Mono (falling back through `ui-monospace` → SFMono-Regular →
Menlo)

All three load from Google Fonts with `preconnect` and `display=swap`. PRODUCT.md documents
why that single external dependency is accepted for a one-file page.

**Character:** One grotesk family doing two jobs at very different weights — Archivo Black set
tight, uppercase and slightly negative-tracked for poster-scale statements, and regular
Archivo at comfortable measure for everything that has to be read. The pairing is deliberately
narrow: the display voice earns its impact from scale, tracking and the pink misregister
rather than from a second typeface. Spline Sans Mono is the third voice and is treated as
evidence, not decoration.

### Hierarchy

- **Display** (400, `clamp(2.5rem, 7.4vw, 6rem)`, line-height 0.87, tracking -0.028em,
  uppercase): the page headline only, with a 4px pink misregister. `hyphens: none` and
  `text-wrap: balance` are both set — at this scale a hyphen or an orphan is a printing fault.
- **Headline** (400, `clamp(1.75rem, 3.4vw, 3rem)`, same line-height and tracking): every band
  heading, each with a 3px misregister. Also the medallion plate names at
  `clamp(1.5rem, 2.6vw, 2.1rem)`, where the plate name *is* the heading — no label sits above
  it.
- **Deck** (500, `clamp(1.0625rem, 1.15vw + 0.8rem, 1.3125rem)`, line-height 1.42, max 46ch):
  the one paragraph under the headline. Blue bold spans inside it.
- **Title** (700, 1.0625rem, tracking -0.01em): framed-block headings — the chooser header,
  the make-ready header, sub-headings in prose. Regular Archivo, not the display face.
- **Body** (400, `clamp(0.975rem, 0.32vw + 0.9rem, 1.0625rem)`, line-height 1.55, max 68ch):
  all prose. Secondary prose drops to 0.875rem / 0.8125rem in framed blocks.
- **Label** (700, 0.6875rem, tracking 0.11em, uppercase): the `.tag` role — section eyebrows,
  the sheet stamp, run-record and code-block headers. Buttons and run-state chips are the same
  idea at slightly looser or tighter tracking (0.075em / 0.08em).
- **Mono** (400/600, 0.8125rem, line-height 1.65–1.7): code blocks, measured values in the run
  record, step counters, and inline identifiers.

### Named Rules

**The Mono-Is-Evidence Rule.** Spline Sans Mono marks something the reader could verify: a
code sample, a file path, an identifier, a counted row. It is never used for a sentence, a
label, or atmosphere. Inline `<code class="mono">` inside prose is correct — that is an
identifier, not prose. A mono paragraph is not.

**The No-Condensed-Fallback Rule.** The display fallback chain must degrade to a real grotesk
and must **never** contain Impact or another condensed system display face. Losing Archivo
Black should cost weight, not proportion. This is a written brand commitment in PRODUCT.md,
not a preference.

**The Measure-Once Rule.** Do not cap a heading's container and the heading separately. An
earlier build put a 58ch cap on the band header block, which silently overrode the heading's
own 22ch and reflowed it wrong; the comment recording that is still in the file. Display sets
22ch, prose sets 60ch inside a band header and 68ch globally, and the container sets nothing.

## Layout

The page is a single sheet. `.sheet` is a centred container at `max-width: 1560px` with a
fluid gutter of `clamp(1.25rem, 3.2vw, 2.75rem)`, reused by the masthead, every band, and the
colophon. Vertical rhythm comes from band padding at `clamp(2.75rem, 5.5vw, 4.75rem)` and a
3px blue rule on top of each band — the bands are separated by ink, not by air.

The hero is a three-column grid: a 190–215px left rail (the ink-separation legend, which
doubles as navigation), a flexible centre pull, and a 320–375px right column holding the
primary action. It collapses in two stages. At **1180px** the rail drops to a full-width row
ordered last and the grid becomes centre + chooser. At **860px** everything stacks, with the
chooser explicitly ordered above the rail so the primary action stays high; the rotated pink
cut plate is dropped and the sun disc shrinks and shifts.

The rest of the breakpoints are per-block rather than a global scale, and that is how the page
is actually built: **940px** for the two code blocks, **900px** for the separation figure and
the scale band, **820px** for the medallion and the cautions grid. Registration crosses are
positioned against the hero at `calc(var(--gut) - 4px)` and are hidden below **860px**, where
there is no longer margin to hang furniture in. The colour bar is a nine-cell 16px strip at
the foot of the colophon.

Internal padding is not tokenized. It clusters around `0.8–1.1rem` vertical and `1rem`
horizontal for framed blocks and rows, and `0.6rem 0.85rem` for the smaller block headers.
Treat those as observed values, not a scale.

### Named Rules

**The Wrap-Your-Prose Rule.** A grid child that carries text must hold that text in a wrapping
element. Inside a `display: grid` list item, every bare text run and every inline `<code>`
becomes its own grid item and the layout shears. The file wraps step text and separation copy
in a `<span>` for exactly this reason, and says so in a comment. This is the single most
common way to break this layout.

**The Furniture Rule.** Registration crosses, the colour bar, and the sheet stamp are press
furniture: they sit at the edges of the sheet, never inside content, and they are all
`aria-hidden`. Furniture may be removed at narrow widths. It may never become content.

## Elevation & Depth

**There are no shadows in this system — not one `box-shadow` in the file.** Depth is ink
order and physical offset, which is what a duplicator actually gives you.

Four mechanisms carry every bit of dimensionality on the page:

1. **Under-ink.** A second impression painted behind an element and offset — the button's
   `::before` at `translate(4px, 4px)`, multiplied, at `z-index: -1`. It reads as a plate that
   printed slightly out of register, not as a drop shadow, because it is a flat saturated ink
   with a hard edge and no blur.
2. **Misregistration.** The display headings' second pink layer at 3–4px, same technique.
3. **Overlap.** Cut plates multiplying over each other and over the type's ground; the stack
   of three domain shims in the scale band, each shifted 10px further right and layered by
   explicit `z-index`.
4. **Shared edges.** Blocks are framed with 3px ink rules and share borders rather than
   floating. The medallion's three plates are one bordered object divided by 3px rules, not
   three cards.

### Named Rules

**The Flat-Ink Rule.** Nothing on this page is lit. No `box-shadow`, no gradient, no blur, no
glow, no `backdrop-filter`. If something needs to come forward, give it a second impression or
a heavier rule.

**The Button-Is-Not-A-Stacking-Context Rule.** `.btn` must **not** carry
`isolation: isolate`. If it becomes a stacking context, its `z-index: -1` under-ink paints
above its own background instead of behind it, and the second ink multiplies across the whole
face. The inverse holds for `.misreg`, which *does* isolate — safely, because it has no
background of its own, so the negative-z-index layer has nothing to land on top of. The
distinction is background, not element type: **isolate an ink layer's parent only when that
parent is transparent.**

## Shapes

The form language is cut paper. Everything is a hard-edged flat plane in a spot ink.

**Corners are square.** `border-radius` appears exactly twice in the file: `50%` on the hero's
sunflower disc, and `border-radius: inherit` on the grain layer inside a cut shape so the
tooth is clipped to the plate (without it, a round plate prints its grain in a box). No other
element is rounded — not buttons, not fields, not chips, not code blocks.

**Two rule weights, and only two.** `--rule: 1px` for internal dividers and light frames;
`--rule-heavy: 3px` for structural frames and band separations. Frames are blue when the block
is a document (chooser, code block, make-ready) and `--ink` when the block is a field or a
data object (medallion, run record, caution, table). Internal dividers inside those blocks are
often alpha-ink hairlines — `rgba(30,79,255,.28)` between separations and chooser rows,
`rgba(16,19,64,.18)` between table rows — rather than the `--rule` token. That mixed practice
is deliberate and consistent in its own terms: token weights for ink rules that read as
printed rules, alpha hairlines for divisions that should recede.

**Grain (the tooth) is applied to areas, never to type.** One noise token, `--grain`: an
inline SVG `feTurbulence` (`fractalNoise`, baseFrequency 0.72, 4 octaves, tiled at 160px) with
the red channel driven into alpha and offset `-0.22` so the lower quarter thresholds out and
the result reads as speckle rather than haze.

**The critical part is how it is composited.** The tooth is a **paper-coloured layer masked by
the noise** (`background: var(--paper)` + `mask-image: var(--grain)`), so paper breaks through
the ink. It is *not* a dark multiply on top: that approach desaturated the light inks by
15–20% and was replaced. Coverage varies by area, and the values in the build are: cut plates
0.34, ink fields / swatches / bullets / colour bar 0.30, the scale-band shims 0.28, table
header cells 0.26, buttons 0.20. Buttons carry the least because type sits directly on them.
Over the whole sheet, `body::before` adds one fixed light darkening fibre at 0.09 with
`mix-blend-mode: multiply` — that one *is* a multiply, and it is the only one.

The separation figure reproduces the same coverage with an SVG filter, and that filter
**must** be pinned `color-interpolation-filters="sRGB"`. With the default linearRGB
compositing it printed roughly 3× heavier than the CSS ink fields and read as a dither rather
than tooth. Its `baseFrequency` is 0.7 against the CSS token's 0.72 — a small unexplained
divergence in the built file, recorded rather than reconciled.

**Halftone screens** are the reduced-density form: a `radial-gradient` dot at a 5px pitch
(`.screen-pink`, `.screen-blue`, dot 1.55px → transparent 1.75px). Used for the sibling
separation swatch and in the colour bar. A third, hand-rolled screen at a slightly larger dot
(1.9px → 2.1px) in sunflower sits inline in the `state-setup` chip rather than going through
the `.screen-*` classes — a small inconsistency in the built system. So is the fact that
`.screen-pink` sets `background-color: transparent` and `.screen-blue` does not; harmless
today because both are only used on elements with no background.

## Components

### Buttons

- **Shape:** square (0 radius), 3px ink frame, `0.85rem 1.35rem` padding, label typography at
  0.8125rem / 0.075em uppercase, with an inline 1em SVG arrow.
- **Primary:** blue field, paper type, pink under-ink offset 4px and multiplied.
- **Ghost:** paper field, blue type, blue frame, sunflower under-ink offset 4px. The face is
  **opaque paper, never transparent** — that is what keeps the second impression visible only
  at the offset instead of showing through the whole button.
- **Hover:** the whole button slips `translate(2px, 2px)` while the under-ink travels to
  `translate(7px, 7px)`, so the visible gap opens from 4px to 5px. 0.18s on
  `cubic-bezier(.16, 1, .3, 1)`.
- **Focus:** the global 3px pink ring at 3px offset. There is **no `:active` state** in the
  built page.
- Every button also carries a grain layer at 0.20 opacity.

### Chips

Two unrelated chip families, both square, both label typography.

- **Sheet stamp** (masthead): 1px blue frame, blue type, `0.3rem 0.55rem`. A `hot` variant
  swaps frame and type to pink.
- **Run state** (separation rail): 1px `currentColor` frame, `0.1rem 0.35rem`, 0.08em
  tracking. Three variants only, and they are an ink-density ramp rather than a status palette
  — see the rule below.

### Cards / Containers

There are no cards. There are **framed blocks**, and they share three traits: square corners,
a 3px frame, and a header row divided from the body by a 1px rule.

- **Document blocks** (chooser, code block, make-ready) are framed in blue on paper.
- **Data blocks** (run record, medallion, caution, table wrapper) are framed in `--ink`.
- **Ink fields** (`.field` + `.f-pink` / `.f-blue` / `.f-sun`) flood a block or a block header
  with a spot ink and carry the grain layer. `.field > *` is forced to
  `position: relative; z-index: 1` so the tooth sits under the type and never multiplies
  across it.
- Internal padding runs `0.8–1.1rem` vertical, `1rem` horizontal.

### Navigation

There is no nav bar. Navigation is the **ink-separation rail** in the hero: five rows, each a
26px ink swatch (solid, screened, or overprint) plus a name, one line of separation metadata
in the lighter run, and a run-state chip. Rows are separated by alpha-blue hairlines. On hover
or focus the name turns pink; the swatch does not change. The masthead carries no links at all
— only the wordmark and the sheet stamp.

The colophon is the secondary navigation: four columns of documentation links, each column
headed by a small blue label above a 1px blue rule. Each link is preceded by a **drawn 2px
pink ink rule** that grows from `scaleX(.64)` to `scaleX(1)` on hover — a mark, not a typed
dash.

### Code blocks

- Blue 3px frame on paper. Header row carries a blue `.tag` label on the left and the source
  file path in mono at 0.6875rem / 0.7 opacity on the right.
- Body is mono at 0.8125rem, line-height 1.65, horizontally scrollable with a thin blue
  scrollbar.
- Four syntax inks: pink keywords (600), blue function names (600), `#4a4f78` comments,
  `#12734a` strings.
- These are the page's evidence. They are never truncated with an ellipsis and never
  decorated.

### Signature: the misregistered display heading

The display voice's signature and the thing that makes the page read as printed. `.misreg`
carries a `data-ink` attribute duplicating its own text; a `::before` renders
`content: attr(data-ink)` in pink, offset by `translate(var(--mx, 3px), var(--my, 3px))`,
multiplied, at `z-index: -1`, inside `isolation: isolate`. The offset is the knob: 2px on the
masthead wordmark, 3px on band headings, 4px on the page headline — the offset scales with the
type, as a real registration error would.

### Signature: the separation figure

Three overlapping ink discs standing for three of the five modules, with all four intersection
regions painted at their declared hex through nested `clipPath`s, and one SVG grain filter
clipped to the group's own alpha so the tooth never prints outside the discs. Labels are
positioned against the *plate* wrapper rather than the figure, so the caption can grow without
dragging a label away from the disc it names. It carries `role="img"` and a full `aria-label`
that states the whole argument in words; the four visual labels are `aria-hidden` duplicates.

### Signature: cut-paper icons

The entire icon language, and there is no icon library or icon font anywhere in the file.
Every glyph is authored inline SVG built from flat geometry — circles, a triangle, a D-shaped
half-round, rectangles, three stacked bars — in two layers, where the second layer (`.l2`)
multiplies over the first. On hover the first layer moves `translate(-2px, -2px)` and the
second moves `translate(4px, 4px)`, opening the registration. Registration crosses and arrows
are drawn the same way, in `currentColor`.

One maintenance consequence: glyph fills are **literal hex** (`fill="#1E4FFF"`), because SVG
presentation attributes cannot read CSS custom properties. A token change must be mirrored in
the glyph SVGs by hand.

### Named Rules

**The Ink-Density State Rule.** Run state is expressed as escalating ink density, never as
invented status colours. Outline blue = ships now (in the deploy glob). Screened sunflower =
needs external setup. Solid pink = customer-run only. Three levels, one grammar; a green /
amber / red palette would be a different design system.

**The Drawn-Marker Rule.** No unicode glyph ever stands in for an icon or a bullet. List
markers are drawn ink rules or authored SVG shapes; step numbers are mono digits in a 1px blue
box. If you find yourself typing a dash, an arrow, or a bullet character, draw it instead.

## Motion

One authored moment, and it is gated: everything below lives inside
`@media (prefers-reduced-motion: no-preference)`.

**`take-ink`** — ink shapes land in sequence, `0.72s` on `cubic-bezier(.16, 1, .3, 1)`, from
`opacity: 0` and a small offset/scale (`translate(var(--ix, -10px), var(--iy, 7px))
scale(.985)`). Applied to the hero's cut plates (delays 0.05s / 0.16s), the scale-band shims
(0.06 / 0.13 / 0.20s, bottom plate first), and the colour bar cells. **`settle-reg`** — `0.8s`,
same easing, brings each misregister layer in from 3.2× its offset into register.

Content is visible by default: only ink layers animate, and the `opacity: 0` start exists
solely inside the reduced-motion-safe branch.

Interaction easing is the same exponential ease-out at a shorter duration: 0.18s for the
button slip, 0.16s for glyph registration and the colophon marker. The one outlier is the
chooser row's sunflower wash, which uses plain `ease-out` at 0.16s.

### Named Rules

**The One-Moment Rule.** The page gets one authored motion idea — the sheet taking ink — and
state changes get the same easing at a shorter duration. Nothing else moves. No scroll
parallax, no reveal-on-scroll, no looping ambient motion.

**Known limitation, honestly recorded.** `take-ink` fires on page load with no scroll trigger,
so instances far down the sheet — the scale-band shims, the colour bar — complete their
animation off-screen and are simply present when the reader arrives. This is a limitation of
the built page, not an intended restraint. Two selectors in the animation block are also dead:
`.venn .disc` and the `.d-blue` / `.d-pink` / `.d-sun` delay classes match no element in the
markup, so **the separation figure's discs never animate at all**.

## Accessibility

The working floor is WCAG 2.1 AA. Measured against the built page:

**Contrast, on the real pairs.** Ink on paper 16.5:1. Lighter run on paper 7.7:1. Code
comments on paper 7.3:1. Blue on paper 5.4:1 (and paper on blue, 5.4:1). Code strings on paper
5.5:1. Ink on pink 5.5:1. Ink on sunflower 12.2:1. Every pair carrying prose, secondary copy,
or code text clears AA, and most clear AAA.

**The one failure: pink text on paper is 3.0:1.** That is below the 4.5:1 needed for normal
text. It affects link hover (`a:hover`), the `hot` sheet-stamp chip at 11px, the separation
name on hover, and — most consequentially, given that PRODUCT.md requires code to stay legible
when projected — the pink keyword colour in code blocks. It does clear the 3:1 bar for large
text and for non-text UI, so the focus ring and the pink field borders are compliant. Pink as
a *field* with ink type on it (5.5:1) is fine. **Pink as small type on paper is the open
item.**

**Present and working:** a skip link, a global `:focus-visible` ring (3px pink, 3px offset)
that no component overrides away, `prefers-reduced-motion` gating on all motion,
`role="img"` plus a complete `aria-label` on the separation figure with its visual labels
`aria-hidden`, `aria-labelledby` on the rail nav, `scope` on every table header plus a
`<caption>`, `<figure>`/`<figcaption>`, semantic landmarks throughout, `lang="en"`, and a
viewport meta that does not block pinch-zoom.

**Known-open items beyond the pink pair:**

- The skip link targets `#chooser` (the primary action) rather than `#main`. `<main id="main">`
  exists and nothing links to it. Sending the reader straight to the connector chooser is a
  defensible choice for this page, but it is not the "skip to content" convention a screen
  reader user expects.
- The horizontally scrollable regions — the code `<pre>` blocks and the comparison table,
  which has `min-width: 640px` — are not keyboard-focusable. A keyboard-only user on a narrow
  viewport cannot scroll them.
- `.misreg::before` uses `content: attr(data-ink)`, and generated content is announced by
  several screen readers. Every display heading may be read twice, and a pseudo-element cannot
  be `aria-hidden`. No mitigation is in the built page.
- Many component rules name a bare `font-family: Archivo` or `font-family: "Spline Sans Mono"`
  with no fallback chain (buttons, state chips, table headers, colophon headings, step
  counters, the figure's labels). The complete ordered chains exist only on `body`, `.display`
  and `.mono`. If the Google Fonts request fails, those UI labels fall back to the browser
  default rather than to the intended grotesk.
- `body { overflow-x: hidden }` is set globally, which suppresses the visible symptom of any
  horizontal overflow rather than preventing it.
- The `src/shared` label inside the separation figure is 0.625rem (10px) paper-on-ink. It is
  `aria-hidden` and its content is carried in the figure's `aria-label`, so nothing is lost to
  assistive tech, but it is very small type.

## Do's and Don'ts

### Do:

- **Do** paint an overprint you can name (`#6A3DFF`, `#FF8A3C`, `#23B36B`) and multiply an
  overlap you can't. Multiply between two saturated spot inks can only go darker.
- **Do** apply grain as a paper-coloured layer masked by `--grain`, at 0.20–0.34 depending on
  area, with lower coverage where type sits directly on the ink.
- **Do** pin `color-interpolation-filters="sRGB"` on any SVG grain filter, or it prints ~3×
  heavy and reads as a dither.
- **Do** express a variant as the same ink at screen (5px pitch) rather than as a new hue.
- **Do** keep `.field > *` at `position: relative; z-index: 1` so the tooth never multiplies
  across type.
- **Do** give every new ink layer's parent `isolation: isolate` **only** when that parent is
  transparent; a parent with its own background must not isolate.
- **Do** carry prose inside a wrapping element when it lives in a `display: grid` child.
- **Do** cap the measure in exactly one place: 22ch on display, 60–68ch on prose, and nothing
  on the container.
- **Do** draw new icons as flat cut-paper geometry in two layers, and mirror any token change
  into their literal hex fills.
- **Do** keep mono for code, paths, identifiers and measured values.

### Don't:

- **Don't** add `isolation: isolate` to `.btn`. The under-ink will paint over the button face.
- **Don't** make a ghost button's face transparent. The second impression must show only at
  the offset.
- **Don't** use `#FFD22A` as a type, stroke, or rule colour. Sunflower is a surface only.
- **Don't** set pink as small text on paper — it is 3.0:1. Use it as a field with ink type, or
  at large sizes, or as a non-text signal.
- **Don't** introduce a fourth spot ink or a green/amber/red status palette. Run state is an
  ink-density ramp: outline blue, screened sunflower, solid pink.
- **Don't** add a `box-shadow`, gradient, blur, or glow. Depth is under-ink, misregistration,
  overlap, and shared edges.
- **Don't** round a corner. The only radii in the system are one 50% disc and the `inherit`
  that clips its grain.
- **Don't** put Impact or any condensed system display face in the fallback chain.
- **Don't** let mono carry a sentence, and don't let a typed dash, arrow, or bullet character
  stand in for a drawn mark.
- **Don't** add a second motion idea, a scroll-triggered reveal, or any ambient loop. One
  authored moment, one easing curve.
- **Don't** put a token helper or a new global rule inside a component block and assume the
  cascade will sort it out — this page has no build step and no specificity safety net.
