import { Weak } from '../../../../helpers'
import { PropsUIPageEnd } from '../../../../types/pages'
import { ReactFactoryContext } from '../../factory'
import { Footer } from './templates/footer'
import { Sidebar } from './templates/sidebar'
import LogoSvg from '../../../../../assets/images/logo.svg'
import { Page } from './templates/page'
import TextBundle from '../../../../text_bundle'
import { Translator } from '../../../../translator'
import { BodyLarge, Title1 } from '../elements/text'

type Props = Weak<PropsUIPageEnd> & ReactFactoryContext

export const EndPage = (props: Props): JSX.Element => {
  const { title, text } = prepareCopy(props)

  const footer: JSX.Element = <Footer />

  const sidebar: JSX.Element = <Sidebar logo={LogoSvg} />

  const body: JSX.Element = (
    <>
      <Title1 text={title} />
      <BodyLarge text={text} />
    </>
  )

  return (
    <Page
      body={body}
      sidebar={sidebar}
      footer={footer}
    />
  )
}

interface Copy {
  title: string
  text: string
}

function prepareCopy ({ locale }: Props): Copy {
  return {
    title: Translator.translate(title, locale),
    text: Translator.translate(text, locale)
  }
}

const title = new TextBundle()
  .add('en', 'Heel erg bedankt voor het meedoen!')
  .add('nl', 'Heel erg bedankt voor het meedoen!')

const text = new TextBundle()
  .add('en', 'Heel erg bedankt dat je mee hebt gedaan met ons onderzoek! Dankzij jou kunnen we de effecten van social media nu nog beter onderzoeken!')
  .add('nl', 'Heel erg bedankt dat je mee hebt gedaan met ons onderzoek! Dankzij jou kunnen we de effecten van social media nu nog beter onderzoeken!')
