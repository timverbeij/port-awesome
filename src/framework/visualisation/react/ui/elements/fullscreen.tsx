import { useEffect, useRef, useState } from 'react'

export const Fullscreen = ({ children, enabled }: { children: JSX.Element; enabled?: boolean }) => {
  const [fullscreen, setFullscreen] = useState<boolean>(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    function handleClickOutside(event: any) {
      if (ref.current && !ref.current.contains(event.target)) {
        setFullscreen(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [ref])

  if (!enabled) return null

  return (
    <>
      <button onClick={() => setFullscreen(true)}>{zoomInIcon}</button>
      {fullscreen ? (
        <div className="fixed inset-0 flex justify-center items-center z-50 ">
          <div
            ref={ref}
            className="relative p-6 z-50 w-[80%] h-[80%] animate-fadeIn bg-white border-2 border-grey2 rounded-md"
          >
            <button
              className="absolute top-0 right-0 py-3 px-5"
              onClick={() => setFullscreen(false)}
            >
              {zoomOutIcon}
            </button>
            {children}
          </div>
        </div>
      ) : null}
    </>
  )
}

const zoomInIcon = (
  <svg
    className="h-6 w-6"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    viewBox="0 0 24 24"
    xmlns="http://www.w3.org/2000/svg"
    aria-hidden="true"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607zM10.5 7.5v6m3-3h-6"
    />
  </svg>
)

const zoomOutIcon = (
  <svg
    className="h-6 w-6"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    viewBox="0 0 24 24"
    xmlns="http://www.w3.org/2000/svg"
    aria-hidden="true"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607zM13.5 10.5h-6"
    />
  </svg>
)
