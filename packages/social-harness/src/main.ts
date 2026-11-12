#!/usr/bin/env node
/** @file The `sharnessd` executable: runs the daemon's command line with the process's arguments. */
import { runCommand } from "./harness/startup.js";

runCommand(process.argv.slice(2));
