# ACF SECURITY — Instalador do Conector de Câmeras

**Monitoramento inteligente. Segurança conectada.**

Este repositório contém os arquivos do conector utilizados pelo ACF SECURITY para integrar câmeras compatíveis com ONVIF à plataforma.

## Componentes

- `install-camera.py` — assistente de instalação e configuração do conector.
- `connector-handler.py` — processamento e envio de eventos para a plataforma.
- `heartbeat.py` — comunicação periódica para informar o estado da instalação.
- `image/` — arquivos necessários para construir a imagem Docker do conector.

## Requisitos

- Servidor Linux com Python 3.
- Docker instalado e em funcionamento.
- Câmera compatível com a integração utilizada.
- Acesso à plataforma ACF SECURITY.
- Credenciais da câmera e dados de instalação fornecidos para a configuração.

## Segurança

- Não publique senhas de câmeras, chaves de instalação, tokens ou arquivos de configuração de clientes.
- Proteja os arquivos que armazenam credenciais e restrinja as permissões de acesso.
- Utilize somente dados de instalação autorizados.
- Antes de executar o instalador em produção, confirme os requisitos e valide o procedimento no ambiente de destino.

## Plataforma

Painel ACF SECURITY: https://acfsecurity.lovable.app

Repositório oficial do instalador: https://github.com/adryanocf/acf-security-installer

## Aviso

Este repositório contém os arquivos do conector. A instalação deve ser realizada em um ambiente autorizado e compatível com os requisitos da integração.
