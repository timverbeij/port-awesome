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
import { zoomInIcon, zoomOutIcon } from '../elements/zoom_icons'

const doubleTypes = ['wordcloud']

type Props = VisualizationProps & ReactFactoryContext

export interface VisualizationProps {
  table: TableWithContext
  visualization: VisualizationType
  locale: string
  handleDelete: (rowIds: string[]) => void
  handleUndo: () => void
}

type ShowStatus = 'hidden' | 'visible' | 'double'

export const Figure = ({
  table,
  visualization,
  locale,
  handleDelete,
  handleUndo
}: Props): JSX.Element => {
  const [visualizationData, status] = useVisualizationData(table, visualization)
  const [longLoading, setLongLoading] = useState<boolean>(false)
  const [showStatus, setShowStatus] = useState<ShowStatus>('visible')
  const canDouble = doubleTypes.includes(visualization.type)
  const [resizeLoading, setResizeLoading] = useState<boolean>(false)

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

  function toggleDouble () {
    setResizeLoading(true)
    if (showStatus === 'visible') {
      setShowStatus('double')
    } else {
      setShowStatus('visible')
    }
    setTimeout(() => {
      setResizeLoading(false)
    }, 150)
  }

  const { title } = useMemo(() => {
    const title = Translator.translate(visualization.title, locale)
    return { title }
  }, [visualization])

  const { errorMsg, noDataMsg } = useMemo(() => prepareCopy(locale), [locale])

  if (visualizationData == null && status === 'loading') {
    return (
      <div className='flex justify-center items-center gap-6'>
        <div className='w-10 h-10'>
          <Lottie animationData={spinnerDark} loop />
        </div>
        <span className='text-grey1'>{title}</span>
      </div>
    )
  }

  if (status === 'error') {
    return <div className='flex justify-center items-center text-error'>{errorMsg}</div>
  }

  let height = visualization.height || 300
  if (showStatus === 'double') height = height * 2

  return (
    <div className='grid'>
      <div
        className={`relative flex flex-col overflow-hidden ${
          status === 'loading' ? 'opacity-50' : ''
        }`}
      >
        <div className='relative z-40 flex justify-between'>
          <Title6 text={title} margin='mt-2 mb-4 z-[-1] relative' />

          <button
            onClick={toggleDouble}
            className={showStatus !== 'hidden' && canDouble ? 'text-primary' : 'hidden'}
          >
            {showStatus === 'double' ? zoomOutIcon : zoomInIcon}
          </button>
        </div>
        <div
          className='transition-all relative z-30 grid max-w-full'
          style={{ gridTemplateRows: height + 'px' }}
        >
          <RenderVisualization
            visualizationData={visualizationData}
            fallbackMessage={noDataMsg}
            loading={resizeLoading}
          />
        </div>
        <div className={`absolute w-8 h-8 top-0 right-0 ${longLoading ? '' : 'hidden'}`}>
          <Lottie animationData={spinnerDark} loop />
        </div>
      </div>
    </div>
  )
}

const RenderVisualization = memo(
  ({
    visualizationData,
    fallbackMessage,
    loading
  }: {
    visualizationData: VisualizationData | undefined
    fallbackMessage: string
    loading: boolean
  }): JSX.Element | null => {
    if (visualizationData == null) return null

    const fallback = (
      <div className='m-auto font-bodybold text-4xl text-grey2 '>{fallbackMessage}</div>
    )

    if (loading) return null

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

function prepareCopy (locale: string): Record<string, string> {
  return {
    errorMsg: Translator.translate(errorMsg, locale),
    noDataMsg: Translator.translate(noDataMsg, locale)
  }
}

const noDataMsg = new TextBundle().add('en', 'No data').add('nl', 'Geen data')

const errorMsg = new TextBundle()
  .add('en', 'Could not create visualization')
  .add('nl', 'Kon visualisatie niet maken')
