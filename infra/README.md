# infra — fast-platform

Estrutura de deploy (Kustomize) deste servico, plana, extraida do que roda no cluster `admin@hellnet`.

- `deployment.yaml`, `service.yaml` (se houver), `config.env` (vira o ConfigMap com hash).
- `kustomization.yaml`: namespace `fast` e tag da imagem.
- `application.yaml`: Application do ArgoCD (aplicar uma vez com `kubectl apply -f`); ignorado pelo Kustomize.

O segredo `fast-database` ja existe no cluster e e referenciado por nome. Nao esta versionado.
Validar: `kubectl kustomize infra`.
