# 灰度发布方案 v2.0

## 概述
本文档描述 Quant Research System v2.0 的灰度发布（金丝雀部署）策略，通过逐步增加流量的方式，降低新版本发布的风险。

## 发布目标
- **最小化风险**: 逐步验证新版本稳定性
- **快速回滚**: 发现问题立即回滚
- **用户无感知**: 平滑过渡，不影响用户体验
- **数据完整性**: 确保数据一致性

---

## 灰度发布架构

### 部署拓扑
```
                    ┌─────────────┐
                    │  Nginx LB   │
                    └──────┬──────┘
                           │
              ┌────────────┴────────────┐
              │                         │
         ┌────▼────┐              ┌────▼────┐
         │ v1.0    │              │ v2.0    │
         │ (稳定版) │              │ (金丝雀) │
         └─────────┘              └─────────┘
         95% 流量                  5% 流量
```

### 流量分配策略
- **基于权重**: Nginx upstream 权重配置
- **基于用户**: 特定用户/IP 路由到新版本
- **基于功能**: 特定 API 路由到新版本

---

## 灰度发布阶段

### 阶段 0: 准备阶段 (D-1)
**时间**: 部署前 1 天
**目标**: 完成所有准备工作

#### 检查清单
- [ ] 代码已合并到 main 分支
- [ ] 所有测试通过（单元/集成/E2E）
- [ ] Staging 环境验证通过
- [ ] 数据库迁移脚本准备完成
- [ ] 回滚方案准备完成
- [ ] 监控告警配置完成
- [ ] 相关人员已通知
- [ ] 部署文档已更新

#### 准备工作
1. **环境准备**
   ```bash
   # 在生产环境部署 v2.0 实例（不接入流量）
   cd /path/to/production
   git clone https://github.com/your-repo/quantsystem.git v2.0
   cd v2.0/backend
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **配置验证**
   ```bash
   # 验证配置文件
   python -c "from app.core.config import settings; print(settings.environment)"
   ```

3. **健康检查**
   ```bash
   # 启动服务（不接入流量）
   python main.py &
   # 健康检查
   curl http://localhost:8001/health
   ```

---

### 阶段 1: 5% 流量 (D0 - D1)
**时间**: 1 天
**目标**: 验证基本功能正常

#### 流量配置
```nginx
# /etc/nginx/conf.d/quant.conf
upstream backend {
    server 127.0.0.1:8000 weight=95;  # v1.0
    server 127.0.0.1:8001 weight=5;   # v2.0
}
```

#### 执行步骤
1. **更新 Nginx 配置**
   ```bash
   sudo vim /etc/nginx/conf.d/quant.conf
   sudo nginx -t
   sudo nginx -s reload
   ```

2. **验证流量分配**
   ```bash
   # 检查访问日志
   tail -f /var/log/nginx/access.log | grep "8001"
   ```

3. **监控指标**
   - 错误率 < 0.1%
   - P95 响应时间 < 500ms
   - 内存使用 < 4GB
   - CPU 使用 < 70%

#### 验证标准
- [ ] 无 5xx 错误
- [ ] 响应时间正常
- [ ] 无内存泄漏
- [ ] 无数据库连接泄漏
- [ ] 核心功能正常（数据同步/因子计算）

#### 回滚条件
- 错误率 > 1%
- P95 响应时间 > 2s
- 内存使用 > 8GB
- 出现严重 Bug

---

### 阶段 2: 25% 流量 (D1 - D3)
**时间**: 2 天
**目标**: 验证中等负载下的稳定性

#### 流量配置
```nginx
upstream backend {
    server 127.0.0.1:8000 weight=75;  # v1.0
    server 127.0.0.1:8001 weight=25;  # v2.0
}
```

#### 执行步骤
1. **更新流量权重**
   ```bash
   sudo vim /etc/nginx/conf.d/quant.conf
   sudo nginx -s reload
   ```

2. **增强监控**
   - 每小时检查一次监控面板
   - 关注业务指标（因子计算成功率、数据同步成功率）
   - 收集用户反馈

#### 验证标准
- [ ] 错误率 < 0.1%
- [ ] 响应时间稳定
- [ ] 无性能退化
- [ ] 业务指标正常
- [ ] 用户无投诉

#### 回滚条件
- 错误率 > 0.5%
- P95 响应时间 > 1s
- 业务指标异常
- 用户投诉增加

---

### 阶段 3: 50% 流量 (D3 - D6)
**时间**: 3 天
**目标**: 验证高负载下的稳定性

#### 流量配置
```nginx
upstream backend {
    server 127.0.0.1:8000 weight=50;  # v1.0
    server 127.0.0.1:8001 weight=50;  # v2.0
}
```

#### 执行步骤
1. **更新流量权重**
   ```bash
   sudo vim /etc/nginx/conf.d/quant.conf
   sudo nginx -s reload
   ```

2. **性能对比**
   - 对比 v1.0 和 v2.0 的性能指标
   - 分析资源使用情况
   - 验证优化效果

#### 验证标准
- [ ] 错误率 < 0.1%
- [ ] 响应时间 ≤ v1.0
- [ ] 资源使用合理
- [ ] 无性能瓶颈
- [ ] 长时间运行稳定（72小时）

#### 回滚条件
- 错误率 > 0.5%
- 性能明显退化
- 资源使用异常
- 出现稳定性问题

---

### 阶段 4: 100% 流量 (D6+)
**时间**: 持续
**目标**: 完全切换到新版本

#### 流量配置
```nginx
upstream backend {
    server 127.0.0.1:8001 weight=100;  # v2.0
    # server 127.0.0.1:8000 weight=0;   # v1.0 (保留但不接入流量)
}
```

#### 执行步骤
1. **完全切换**
   ```bash
   sudo vim /etc/nginx/conf.d/quant.conf
   sudo nginx -s reload
   ```

2. **持续监控**
   - 前 24 小时密切监控
   - 前 7 天每天检查
   - 保留 v1.0 实例 7 天以备回滚

3. **清理旧版本**
   ```bash
   # 7 天后，确认无问题
   sudo systemctl stop quant-v1.0
   sudo rm -rf /path/to/v1.0
   ```

#### 验证标准
- [ ] 所有指标正常
- [ ] 用户反馈良好
- [ ] 无遗留问题
- [ ] 文档已更新

---

## 监控指标

### 关键指标
| 指标 | 阈值 | 告警级别 |
|------|------|----------|
| 错误率 | > 0.1% | Warning |
| 错误率 | > 1% | Critical |
| P95 响应时间 | > 500ms | Warning |
| P95 响应时间 | > 2s | Critical |
| 内存使用 | > 80% | Warning |
| 内存使用 | > 90% | Critical |
| CPU 使用 | > 70% | Warning |
| CPU 使用 | > 85% | Critical |

### 业务指标
| 指标 | 阈值 | 告警级别 |
|------|------|----------|
| 因子计算成功率 | < 95% | Warning |
| 因子计算成功率 | < 90% | Critical |
| 数据同步成功率 | < 95% | Warning |
| 数据同步成功率 | < 90% | Critical |
| API 可用性 | < 99.9% | Critical |

### 监控面板
- **Grafana Dashboard**: http://grafana.example.com/d/quant-v2
- **Prometheus**: http://prometheus.example.com
- **日志聚合**: http://kibana.example.com

---

## 回滚流程

### 自动回滚触发条件
- 错误率 > 5% 持续 5 分钟
- P95 响应时间 > 10s 持续 5 分钟
- 服务不可用

### 手动回滚步骤
1. **立即切换流量**
   ```bash
   # 将所有流量切回 v1.0
   sudo vim /etc/nginx/conf.d/quant.conf
   # 修改 weight: v1.0=100, v2.0=0
   sudo nginx -s reload
   ```

2. **停止 v2.0 服务**
   ```bash
   sudo systemctl stop quant-v2.0
   ```

3. **执行数据库回滚**
   ```bash
   cd /path/to/v2.0/backend
   source .venv/bin/activate
   python database/migrations/v2.0/rollback.py
   ```

4. **验证回滚**
   ```bash
   # 健康检查
   curl http://localhost:8000/health
   # 冒烟测试
   bash scripts/deploy/smoke_test.sh
   ```

5. **通知相关人员**
   - 发送回滚通知
   - 说明回滚原因
   - 制定修复计划

---

## 决策矩阵

### 进入下一阶段的条件
| 阶段 | 必须满足的条件 |
|------|---------------|
| 1 → 2 | 运行 24 小时无严重问题 |
| 2 → 3 | 运行 48 小时，所有指标正常 |
| 3 → 4 | 运行 72 小时，性能优于或等于 v1.0 |

### 回滚决策
| 问题严重程度 | 影响范围 | 决策 | 决策人 |
|-------------|---------|------|--------|
| P0 - 致命 | 全部用户 | 立即回滚 | 值班工程师 |
| P1 - 严重 | > 50% 用户 | 15分钟内回滚 | 技术负责人 |
| P2 - 一般 | < 50% 用户 | 评估后决定 | 技术负责人 |
| P3 - 轻微 | < 10% 用户 | 继续观察 | 值班工程师 |

---

## 沟通计划

### 发布前通知
- **时间**: 部署前 24 小时
- **对象**: 全体员工、用户（如需要）
- **内容**: 发布时间、预期影响、联系方式

### 发布中通知
- **频率**: 每个阶段开始时
- **对象**: 技术团队、运维团队
- **内容**: 当前阶段、监控指标、注意事项

### 发布后总结
- **时间**: 完全切换后 1 周
- **对象**: 全体技术团队
- **内容**: 发布过程回顾、问题总结、改进建议

---

## 应急联系人

| 角色 | 姓名 | 电话 | 邮箱 | 职责 |
|------|------|------|------|------|
| 技术负责人 | ___ | ___ | ___ | 最终决策 |
| 运维负责人 | ___ | ___ | ___ | 部署执行 |
| 值班工程师 | ___ | ___ | ___ | 监控告警 |
| DBA | ___ | ___ | ___ | 数据库操作 |

---

## 附录

### A. Nginx 配置示例
```nginx
# /etc/nginx/conf.d/quant.conf
upstream backend {
    # v1.0 稳定版
    server 127.0.0.1:8000 weight=95 max_fails=3 fail_timeout=30s;

    # v2.0 金丝雀版本
    server 127.0.0.1:8001 weight=5 max_fails=3 fail_timeout=30s;

    keepalive 32;
}

server {
    listen 80;
    server_name quant.example.com;

    location / {
        proxy_pass http://backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

        # 添加版本标识
        add_header X-Backend-Version $upstream_addr;
    }
}
```

### B. 监控脚本
```bash
#!/bin/bash
# monitor_canary.sh - 监控金丝雀部署

V1_URL="http://localhost:8000/health"
V2_URL="http://localhost:8001/health"

while true; do
    V1_STATUS=$(curl -s -o /dev/null -w "%{http_code}" $V1_URL)
    V2_STATUS=$(curl -s -o /dev/null -w "%{http_code}" $V2_URL)

    echo "[$(date)] v1.0: $V1_STATUS, v2.0: $V2_STATUS"

    if [ "$V2_STATUS" != "200" ]; then
        echo "WARNING: v2.0 health check failed!"
        # 发送告警
    fi

    sleep 30
done
```

### C. 流量分析脚本
```bash
#!/bin/bash
# analyze_traffic.sh - 分析流量分配

tail -f /var/log/nginx/access.log | awk '{
    if ($0 ~ /8000/) v1++
    if ($0 ~ /8001/) v2++
    total = v1 + v2
    if (total > 0) {
        printf "\rv1.0: %d (%.1f%%), v2.0: %d (%.1f%%)",
               v1, v1*100/total, v2, v2*100/total
    }
}'
```

---

**文档版本**: v1.0
**最后更新**: 2026-03-07
**维护人**: DevOps Team
