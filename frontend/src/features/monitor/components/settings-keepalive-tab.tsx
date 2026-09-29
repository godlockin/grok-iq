import { useCallback, useEffect, useState } from 'react'
import { Flame, Play, RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { api, type KeepAliveStatus } from '@/lib/api'
import { getErrorMessage } from '@/lib/utils'
import { Field, NumberField, SettingsCard, SwitchRow } from './settings-components'
import type { SettingsForm, SettingsSetter } from './settings-model'

export function SettingsKeepAliveTab({
  form,
  set,
}: {
  form: SettingsForm
  set: SettingsSetter
}) {
  const [status, setStatus] = useState<KeepAliveStatus | null>(null)
  const [running, setRunning] = useState(false)

  const refresh = useCallback(async () => {
    try {
      setStatus(await api.keepaliveStatus())
    } catch {
      // The panel is informational; a failed poll must not break the settings
      // form the operator is editing.
    }
  }, [])

  useEffect(() => {
    // Subscribing to a polled external source is the intended use of an
    // effect. The initial fetch is async, so setState runs on a later tick
    // rather than synchronously during the effect.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    void refresh()
    const timer = window.setInterval(() => void refresh(), 30_000)
    return () => window.clearInterval(timer)
  }, [refresh])

  const runNow = async () => {
    setRunning(true)
    try {
      const result = await api.runKeepaliveNow()
      await refresh()
      if (result.skipped === 'disabled') {
        toast.info('保活未启用')
        return
      }
      if (result.skipped === 'none_due') {
        toast.info('当前没有到期的账号，账号按各自随机时间触发')
        return
      }
      if (result.skipped === 'no_enabled_accounts') {
        toast.warning('grok2api 没有可用账号')
        return
      }
      toast.success(
        `保活完成：成功 ${result.succeeded}，失败 ${result.failed}`
      )
    } catch (error) {
      toast.error(getErrorMessage(error))
    } finally {
      setRunning(false)
    }
  }

  // Backend rejects min > max; mirror the rule here so the operator gets
  // immediate feedback instead of a round-trip validation error.
  const intervalInvalid = form.keepaliveMinIntervalSeconds > form.keepaliveMaxIntervalSeconds

  return (
    <SettingsCard
      icon={Flame}
      title='账号保活'
      description='按随机时间和随机内容发送低成本对话，让上游看不到账号长期闲置。'
    >
      <div className='space-y-4'>
        <SwitchRow
          label='启用保活'
          description='为所有 grok2api 已启用账号保活，调度时间与内容均随机化'
          checked={form.keepaliveEnabled}
          onCheckedChange={(value) => set('keepaliveEnabled', value)}
        />

        <div className='grid gap-4 sm:grid-cols-2'>
          <NumberField
            label='最小间隔（秒）'
            hint='每个账号下次保活的最早等待时间'
            value={form.keepaliveMinIntervalSeconds}
            min={60}
            max={604800}
            step={60}
            onChange={(value) => set('keepaliveMinIntervalSeconds', value)}
          />
          <NumberField
            label='最大间隔（秒）'
            hint={
              intervalInvalid
                ? '最大间隔不能小于最小间隔'
                : '实际间隔在最小与最大值之间随机选取'
            }
            value={form.keepaliveMaxIntervalSeconds}
            min={60}
            max={604800}
            step={60}
            onChange={(value) => set('keepaliveMaxIntervalSeconds', value)}
          />
          <NumberField
            label='单轮账号上限'
            hint='每轮最多处理的到期账号数，防止大账号池集中触发'
            value={form.keepaliveBatchSize}
            min={1}
            max={500}
            onChange={(value) => set('keepaliveBatchSize', value)}
          />
          <NumberField
            label='并发数'
            hint='同时发起的保活请求上限'
            value={form.keepaliveWorkerConcurrency}
            min={1}
            max={32}
            onChange={(value) => set('keepaliveWorkerConcurrency', value)}
          />
          <NumberField
            label='调度轮询间隔（秒）'
            hint='只是检查频率；实际发送时间由每个账号的随机到期时间决定'
            value={form.keepaliveTickSeconds}
            min={10}
            max={3600}
            onChange={(value) => set('keepaliveTickSeconds', value)}
          />
          <NumberField
            label='失败退避（秒）'
            hint='失败账号的冷却时间，按上游 Retry-After 优先'
            value={form.keepaliveFailureBackoffSeconds}
            min={60}
            max={86400}
            step={60}
            onChange={(value) => set('keepaliveFailureBackoffSeconds', value)}
          />
          <NumberField
            label='单次输出上限'
            hint='保活不计正确性，限制输出以降低成本'
            value={form.keepaliveMaxOutputTokens}
            min={32}
            max={4096}
            step={10}
            onChange={(value) => set('keepaliveMaxOutputTokens', value)}
          />
          <Field label='保活模型' hint='用于保活的 grok_build 模型'>
            <Input
              value={form.keepaliveModel}
              onChange={(event) => set('keepaliveModel', event.target.value)}
            />
          </Field>
        </div>

        <div className='rounded-lg border bg-muted/25 p-3 text-xs leading-5 text-muted-foreground'>
          保活流量写入独立的 keepalive
          表，不产生探针样本，因此不会影响降智评分、账号健康度或探针看板。保活不校验回答内容。
        </div>

        <div className='flex flex-wrap items-center justify-between gap-3 rounded-lg border p-3'>
          <div className='text-xs leading-5 text-muted-foreground'>
            {status ? (
              <span>
                已跟踪 {status.trackedAccounts} 个账号 · 近{' '}
                {status.lookbackHours} 小时成功 {status.succeeded}、失败{' '}
                {status.failed}
              </span>
            ) : (
              <span>状态加载中…</span>
            )}
          </div>
          <div className='flex gap-2'>
            <Button
              variant='outline'
              size='sm'
              onClick={() => void refresh()}
              disabled={running}
            >
              <RefreshCw />
              刷新
            </Button>
            <Button size='sm' onClick={() => void runNow()} disabled={running}>
              <Play />
              立即执行一轮
            </Button>
          </div>
        </div>
      </div>
    </SettingsCard>
  )
}
