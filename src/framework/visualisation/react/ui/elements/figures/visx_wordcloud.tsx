import { Text } from '@visx/text'
import Wordcloud from '@visx/wordcloud/lib/Wordcloud'
import { ParentSize } from '@visx/responsive'
import { TextVisualizationData } from '../../../../../types/visualizations'
import { useMemo } from 'react'

interface Props {
  visualizationData: TextVisualizationData
}

function VisxWordcloud({ visualizationData }: Props): JSX.Element | null {
  const fontRange = [12, 45]
  const colors = ['#1E3FCC', '#4272EF', '#CC9F3F', '#FFCF60']
  const nWords = 100

  const words = useMemo(() => {
    const words = visualizationData.topTerms.slice(0, nWords)

    let minImportance = words[0].importance
    let maxImportance = words[0].importance
    words.forEach((w) => {
      if (w.importance < minImportance) minImportance = w.importance
      if (w.importance > maxImportance) maxImportance = w.importance
    })

    words.forEach((w) => {
      w.importance = (w.importance - minImportance) / (maxImportance - minImportance + 0.001)
    })

    return words
  }, [visualizationData, nWords])

  return (
    <ParentSize debounceTime={1000}>
      {(parent) => (
        <Wordcloud
          words={words}
          height={parent.height}
          width={parent.width}
          rotate={0}
          padding={5}
          spiral="rectangular"
          fontSize={(w) => w.importance * (fontRange[1] - fontRange[0]) + fontRange[0]}
          random={() => 0.5}
        >
          {(cloudWords) => {
            return cloudWords.map((w, i: number) => {
              return (
                <Text
                  key={w.text}
                  fill={colors[Math.floor((i / cloudWords.length) * colors.length)]}
                  fontSize={w.size}
                  textAnchor="middle"
                  fontFamily={w.font}
                  transform={`translate(${w.x ?? 0}, ${w.y ?? 0}) rotate(${w.rotate ?? 0})`}
                >
                  {w.text}
                </Text>
              )
            })
          }}
        </Wordcloud>
      )}
    </ParentSize>
  )
}

export default VisxWordcloud
