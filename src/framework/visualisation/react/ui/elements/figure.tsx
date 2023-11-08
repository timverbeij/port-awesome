import { TableWithContext } from '../../../../types/elements'

import TextBundle from '../../../../text_bundle'
import { Translator } from '../../../../translator'
import {
  VisualizationType,
  VisualizationData,
  ChartVisualizationData,
  TextVisualizationData
} from '../../../../types/visualizations'

import { memo, useEffect, useMemo, useState } from 'react'

import { ReactFactoryContext } from '../../factory'

import useVisualizationData from '../hooks/useVisualizationData'
import { Title6 } from './text'

import Lottie from 'lottie-react'
import spinnerDark from '../../../../../assets/lottie/spinner-dark.json'
import RechartsGraph from './figures/recharts_graph'
import VisxWordcloud from './figures/visx_wordcloud'
import { Fullscreen } from './fullscreen'

const fullscreenTypes = ['wordcloud']

type Props = VisualizationProps & ReactFactoryContext

export interface VisualizationProps {
  table: TableWithContext
  visualization: VisualizationType
  locale: string
  handleDelete: (rowIds: string[]) => void
  handleUndo: () => void
}

export const Figure = ({
  table,
  visualization,
  locale,
  handleDelete,
  handleUndo
}: Props): JSX.Element => {
  const [visualizationData, status] = useVisualizationData(table, visualization)
  const [longLoading, setLongLoading] = useState<boolean>(false)

  const canFullscreen = fullscreenTypes.includes(visualization.type)

  useEffect(() => {
    if (status !== 'loading') {
      setLongLoading(false)
      return
    }
    const timer = setTimeout(() => {
      setLongLoading(true)
    }, 1000)
    return () => clearTimeout(timer)
  }, [status])

  const { title } = useMemo(() => {
    const title = Translator.translate(visualization.title, locale)
    return { title }
  }, [visualization])

  const { errorMsg, noDataMsg } = useMemo(() => prepareCopy(locale), [locale])

  if (visualizationData == null && status === 'loading') {
    return (
      <div className="flex justify-center items-center gap-6">
        <div className="w-10 h-10">
          <Lottie animationData={spinnerDark} loop />
        </div>
        <span className="text-grey1">{title}</span>
      </div>
    )
  }

  if (status === 'error') {
    return <div className="flex justify-center items-center text-error">{errorMsg}</div>
  }

  const visualizationHeightTruthy = Boolean(visualization.height)
  const minHeight = visualizationHeightTruthy ? `${visualization.height ?? ''} px` : '20rem'

  return (
    <div
      className={`relative flex flex-col overflow-hidden ${
        status === 'loading' ? 'opacity-50' : ''
      }`}
    >
      <div className="relative z-40 flex justify-between">
        <Title6 text={title} margin="mt-2 mb-4" />
        <Fullscreen enabled={canFullscreen}>
          <RenderVisualization visualizationData={visualizationData} fallbackMessage={noDataMsg} />
        </Fullscreen>
      </div>
      <div
        className="relative z-30 flex max-w-full"
        style={{ flex: `1 1 ${minHeight}`, minHeight }}
      >
        <RenderVisualization visualizationData={visualizationData} fallbackMessage={noDataMsg} />
      </div>
      <div className={`absolute w-8 h-8 top-0 right-0 ${longLoading ? '' : 'hidden'}`}>
        <Lottie animationData={spinnerDark} loop />
      </div>
    </div>
  )
}

const RenderVisualization = memo(
  ({
    visualizationData,
    fallbackMessage
  }: {
    visualizationData: VisualizationData | undefined
    fallbackMessage: string
  }): JSX.Element | null => {
    if (visualizationData == null) return null

    const fallback = (
      <div className="m-auto font-bodybold text-4xl text-grey2 ">{fallbackMessage}</div>
    )

    if (['line', 'bar', 'area'].includes(visualizationData.type)) {
      const chartVisualizationData: ChartVisualizationData =
        visualizationData as ChartVisualizationData
      if (chartVisualizationData.data.length === 0) return fallback
      return <RechartsGraph visualizationData={chartVisualizationData} />
    }

    if (visualizationData.type === 'wordcloud') {
      const textVisualizationData: TextVisualizationData = visualizationData
      if (textVisualizationData.topTerms.length === 0) return fallback
      return <VisxWordcloud visualizationData={textVisualizationData} />
    }

    return null
  }
)

function prepareCopy(locale: string): Record<string, string> {
  return {
    errorMsg: Translator.translate(errorMsg, locale),
    noDataMsg: Translator.translate(noDataMsg, locale)
  }
}

const noDataMsg = new TextBundle().add('en', 'No data').add('nl', 'Geen data')

const errorMsg = new TextBundle()
  .add('en', 'Could not create visualization')
  .add('nl', 'Kon visualisatie niet maken')
