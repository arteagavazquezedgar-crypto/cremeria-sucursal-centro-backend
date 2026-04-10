# Backend del Administrador remoto listo para Render

## Qué subir a GitHub
Sube **solo** el contenido de esta carpeta `backend` a un repositorio nuevo y privado.

Archivos mínimos:
- `server.js`
- `package.json`
- `package-lock.json`
- `render.yaml`
- `.gitignore`
- `.env.render.example`

## Opción rápida con Blueprint (`render.yaml`)
1. Crea el repo en GitHub con estos archivos en la raíz.
2. En Render entra a **New > Blueprint**.
3. Conecta el repositorio.
4. Revisa la configuración y crea el servicio.
5. Cuando Render termine, usa la URL pública `https://tu-servicio.onrender.com`.

## Opción manual en Render
Si prefieres crear el servicio manualmente:
- Tipo: **Web Service**
- Runtime: **Node**
- Build Command: `npm install`
- Start Command: `npm start`
- Health Check Path: `/health`
- Plan: **Starter** o superior

### Variables de entorno
- `DATA_DIR=/var/data/cremeria`
- `ADMIN_NAME=Cremería El Güero - Sucursal Centro`
- `NODE_VERSION=22`

### Disco persistente
Agrega un disco persistente con:
- Mount Path: `/var/data`
- Size: `1 GB` o más

## Después del deploy
1. Abre la URL pública del servicio y agrega `/health`.
2. Si responde correctamente, esa será la URL que vas a registrar en el Administrador maestro.
3. En la sucursal remota abre el Administrador, ve a **Seguridad** y copia la API key remota o habilita credenciales.
4. En el Administrador maestro registra la sucursal con su URL y credenciales/token.
5. Pulsa **Probar conexión**.

## Nota importante
Este backend guarda datos en archivos locales. Sin disco persistente, Render perderá esos datos al reiniciar o redeployar el servicio.
