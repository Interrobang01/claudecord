module.exports = {
  apps: [{
    name: "claudecord",
    script: "./start.sh",
    cwd: __dirname,
    autorestart: true,
    max_restarts: 10,
    restart_delay: 5000,
    watch: false,
    // DISCORD_TOKEN can be injected at pm2 start time, e.g. via 1Password:
    //   DISCORD_TOKEN=$(op read "op://Vault/Discord Bot Token/credential") pm2 start ecosystem.config.cjs
    // pm2 persists the env across restarts — no repeated secret prompts.
    // Or just put it in .env and let start.sh load it.
  }],
};
