#!/bin/bash

# Docker 镜像拉取脚本 - 使用国内镜像源

set -e

echo "=========================================="
echo "  配置 Docker 镜像加速"
echo "=========================================="
echo ""

# 检查操作系统
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "检测到 macOS 系统"
    echo ""
    echo "请按以下步骤配置 Docker 镜像加速："
    echo ""
    echo "1. 打开 Docker Desktop"
    echo "2. 点击右上角设置图标 (齿轮)"
    echo "3. 选择 'Docker Engine'"
    echo "4. 在 JSON 配置中添加以下内容："
    echo ""
    echo '{'
    echo '  "registry-mirrors": ['
    echo '    "https://docker.mirrors.ustc.edu.cn",'
    echo '    "https://hub-mirror.c.163.com",'
    echo '    "https://mirror.ccs.tencentyun.com"'
    echo '  ]'
    echo '}'
    echo ""
    echo "5. 点击 'Apply & Restart'"
    echo ""
    read -p "配置完成后按 Enter 继续..."
else
    echo "检测到 Linux 系统"
    echo "正在配置 Docker 镜像加速..."

    # 创建或修改 daemon.json
    sudo mkdir -p /etc/docker

    cat <<EOF | sudo tee /etc/docker/daemon.json
{
  "registry-mirrors": [
    "https://docker.mirrors.ustc.edu.cn",
    "https://hub-mirror.c.163.com",
    "https://mirror.ccs.tencentyun.com",
    "https://docker.xuanyuan.me"
  ]
}
EOF

    # 重启 Docker
    sudo systemctl daemon-reload
    sudo systemctl restart docker

    echo "✓ Docker 镜像加速配置完成"
fi

echo ""
echo "=========================================="
echo "  拉取 PostgreSQL 镜像"
echo "=========================================="
echo ""

# 尝试从不同的镜像源拉取
MIRRORS=(
    "docker.mirrors.ustc.edu.cn"
    "hub-mirror.c.163.com"
    "mirror.ccs.tencentyun.com"
)

IMAGE="postgres:16-alpine"
SUCCESS=false

for MIRROR in "${MIRRORS[@]}"; do
    echo "尝试从 $MIRROR 拉取镜像..."

    if docker pull $MIRROR/library/$IMAGE 2>/dev/null; then
        echo "✓ 成功从 $MIRROR 拉取镜像"

        # 重新标记镜像
        docker tag $MIRROR/library/$IMAGE $IMAGE
        echo "✓ 镜像已标记为 $IMAGE"

        SUCCESS=true
        break
    else
        echo "✗ 从 $MIRROR 拉取失败，尝试下一个..."
    fi
done

if [ "$SUCCESS" = false ]; then
    echo ""
    echo "❌ 所有镜像源都无法访问"
    echo ""
    echo "替代方案："
    echo "1. 使用本地 PostgreSQL 安装（推荐）"
    echo "2. 使用 VPN 后重试"
    echo "3. 手动下载镜像文件"
    echo ""
    exit 1
fi

echo ""
echo "=========================================="
echo "  镜像拉取成功"
echo "=========================================="
echo ""
echo "现在可以运行: docker-compose up -d"
echo ""
