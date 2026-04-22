#!/usr/bin/env -S bash --norc --noprofile
cd "$(dirname "$0")"
exec node --import=tsx src/index.ts
