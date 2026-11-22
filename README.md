# Fifoot Online — Deploy 100% na Vercel

Este pacote faz o **Fifoot Online** funcionar 100% na **Vercel** (incluindo aba anônima, 4G, celular e lista global de salas abertas), sem precisar de Render nem de `npm install`.

## Arquivos incluídos
- `index.html` — Jogo completo (263 jogadores calibrados, 8 formações, táticas com peso real, substituições, súmula estilo FIFA, trocas livres, modo vs CPU e adaptador Vercel Online).
- `crests/` — 40 escudos oficiais em PNG (20 clubes do Brasileirão + 20 clubes europeus), com fallback automático em SVG.
- `api/sync.js` — Serverless Function da Vercel que sincroniza salas, sorteio, trocas e partidas.
- `vercel.json` — Configuração de cache da API.

## Como subir no GitHub (até pelo celular)
1. Suba o `index.html`, o `vercel.json` e a pasta `crests/` na raiz do seu repositório.
2. Para criar a pasta `api` pelo celular no GitHub:
   - Toque em **Add file -> Create new file**.
   - No nome do arquivo, digite exatamente: `api/sync.js` (a barra `/` cria a pasta `api` automaticamente).
   - Cole o conteúdo de `api/sync.js` e faça **Commit changes**.

## Passo obrigatório na Vercel (1 clique, grátis)
Para que as salas funcionem entre dispositivos diferentes e aba anônima:
1. Abra seu projeto na **Vercel** -> aba **Storage**.
2. Clique em **Create Database** -> escolha **Upstash Redis** (plano gratuito) e conecte ao projeto.
3. Volte em **Deployments**, abra o deploy mais recente e clique em **Redeploy**.

Pronto! Quando abrir o site, o indicador no topo vai mostrar **Vercel Online (Redis)**.
