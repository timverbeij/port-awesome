import React from 'react'
import { Weak } from '../../../../helpers'
import TextBundle from '../../../../text_bundle'
import { Translator } from '../../../../translator'
import { PropsUIPageSplashScreen } from '../../../../types/pages'
import { ReactFactoryContext } from '../../factory'
import { PrimaryButton } from '../elements/button'
import { CheckBox } from '../elements/check_box'
import { Label, Title1 } from '../elements/text'
import LogoSvg from '../../../../../assets/images/logo.svg'
import { Footer } from './templates/footer'
import { Page } from './templates/page'
import { Sidebar } from './templates/sidebar'
import { Bullet } from '../elements/bullet'

interface Copy {
  title: string
  continueButton: string
  privacyLabel: string
}

type Props = Weak<PropsUIPageSplashScreen> & ReactFactoryContext

function prepareCopy ({ locale }: Props): Copy {
  return {
    title: Translator.translate(title, locale),
    continueButton: Translator.translate(continueButton, locale),
    privacyLabel: Translator.translate(privacyLabel, locale)
  }
}

export const SplashScreen = (props: Props): JSX.Element => {
  const [checked, setChecked] = React.useState<boolean>(false)
  const [waiting, setWaiting] = React.useState<boolean>(false)
  const { title, continueButton, privacyLabel } = prepareCopy(props)
  const { locale, resolve } = props

  function handleContinue (): void {
    if (checked && !waiting) {
      setWaiting(true)
      resolve?.({ __type__: 'PayloadVoid', value: undefined })
    }
  }

  function handleCheck (): void {
    setChecked(true)
  }

  function renderDescription (): JSX.Element {
    if (locale === 'nl') return nlDescription
    return enDescription
  }

    const enDescription: JSX.Element = (
    <>
      <div className='text-bodylarge font-body text-grey1'>
        <div className='mb-4 text-bodylarge font-body text-grey1'>
          Wat leuk dat je mee wilt doen met ons TikTok onderzoek!
        </div>
        <div className='mb-4 text-bodylarge font-body text-grey1'>
          Via deze website kan jij je TikTok data veilig uploaden en doneren.
        </div>
        <div className='mb-6 text-bodylarge font-body text-grey1'>
          Als het goed is, heb je al eerder toestemming gegeven om mee te doen met dit onderzoek. Heb je dit toch nog niet gedaan? Neem dan contact met ons op via <strong>info@project-awesome.nl</strong> of via <strong>WhatsApp (06-12345678)</strong>.
        </div>
        <div className='mb-6 text-bodylarge font-body text-grey1'>
          Om toch nog even kort samen te vatten:
        </div>
        <div className='flex flex-col gap-3 mb-6'>
          <Bullet>
            <div>Je doet vrijwillig mee met dit onderzoek</div>
          </Bullet>
          <Bullet>
            <div>Je bent altijd anoniem</div>
          </Bullet>
          <Bullet>
            <div>Je kan altijd aan ons vragen om je data toch te verwijderen</div>
          </Bullet>
        </div>
      </div>
    </>
  )

  const nlDescription: JSX.Element = (
    <>
      <div className='text-bodylarge font-body text-grey1'>
        <div className='mb-4 text-bodylarge font-body text-grey1'>
          Wat leuk dat je mee wilt doen met ons TikTok onderzoek!
        </div>
        <div className='mb-4 text-bodylarge font-body text-grey1'>
          Via deze website kan jij je TikTok data veilig uploaden en doneren.
        </div>
        <div className='mb-6 text-bodylarge font-body text-grey1'>
          Als het goed is, heb je al eerder toestemming gegeven om mee te doen met dit onderzoek. Heb je dit toch nog niet gedaan? Neem dan contact met ons op via info@project-awesome.nl of via WhatsApp (06-12345678).
        </div>
        <div className='mb-6 text-bodylarge font-body text-grey1'>
          Om toch nog even kort samen te vatten:
        </div>
        <div className='flex flex-col gap-3 mb-6'>
          <Bullet>
            <div>Je doet vrijwillig mee met dit onderzoek</div>
          </Bullet>
          <Bullet>
            <div>Je bent altijd anoniem</div>
          </Bullet>
          <Bullet>
            <div>Je kan altijd aan ons vragen om je data toch te verwijderen</div>
          </Bullet>
        </div>
      </div>
    </>
  )

  const footer: JSX.Element = <Footer />

  const sidebar: JSX.Element = <Sidebar logo={LogoSvg} />

  const body: JSX.Element = (
    <>
      <Title1 text={title} />
      {renderDescription()}
      <div className='flex flex-col gap-8'>
        <div className='flex flex-row gap-4 items-center'>
          <CheckBox id='0' selected={checked} onSelect={() => handleCheck()} />
          <Label text={privacyLabel} />
        </div>
        <div className={`flex flex-row gap-4 ${checked ? '' : 'opacity-30'}`}>
          <PrimaryButton label={continueButton} onClick={handleContinue} enabled={checked} spinning={waiting} />
        </div>
      </div>
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

const title = new TextBundle()
  .add('en', 'Welkom bij het Awesome Science TikTok Onderzoek!')
  .add('nl', 'Welkom bij het Awesome Science TikTok Onderzoek!')

const continueButton = new TextBundle()
  .add('en', 'Ja, ik doe mee!')
  .add('nl', 'Ja, ik doe mee!')

const privacyLabel = new TextBundle()
  .add('en', 'Ik heb de tekst goed gelezen')
  .add('nl', 'Ik heb de tekst goed gelezen')
