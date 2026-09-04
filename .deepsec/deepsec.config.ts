import { defineConfig } from "deepsec/config";

export default defineConfig({
  projects: [
    { id: "membrane", root: ".." },
    // <deepsec:projects-insert-above>
  ],
});
