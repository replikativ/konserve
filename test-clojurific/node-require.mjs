// cljs.nodejs and cljs-node-io use CommonJS require during module initialization.
import { createRequire } from 'node:module';

globalThis.require = createRequire(import.meta.url);
