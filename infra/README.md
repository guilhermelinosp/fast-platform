# infra — fast-platform

Estrutura de deploy deste servico (Kustomize), extraida do que roda no cluster `admin@hellnet`.

- `base/` Deployment, Service e ConfigMap (`config.env` vira o ConfigMap com hash).
- `overlays/homelab/` namespace `fast` e a tag da imagem.
- `argocd/application.yaml` Application do ArgoCD (aplicar uma vez com `kubectl apply -f`).

O segredo `fast-database` ja existe no cluster e e referenciado por nome. Nao esta versionado.

Deploy: workflow `argocd` (manual) pela tailnet, ou edite `newTag` no overlay e faca merge.
Validar localmente: `kubectl kustomize infra/overlays/homelab`.
