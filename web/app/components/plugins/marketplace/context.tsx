'use client'

import type { ReactNode } from 'react'
import {
  useCallback,
  useState,
} from 'react'
import {
  createContext,
  useContextSelector,
} from 'use-context-selector'
import { useDebounceFn } from 'ahooks'
import { PLUGIN_TYPE_SEARCH_MAP } from './plugin-type-switch'
import type { Plugin } from '../types'
import type { PluginsSearchParams } from './types'
import { getMarketplacePlugins } from './utils'

export type MarketplaceContextValue = {
  intersected: boolean
  setIntersected: (intersected: boolean) => void
  searchPluginText: string
  handleSearchPluginTextChange: (text: string) => void
  filterPluginTags: string[]
  handleFilterPluginTagsChange: (tags: string[]) => void
  activePluginType: string
  handleActivePluginTypeChange: (type: string) => void
  plugins?: Plugin[]
  setPlugins?: (plugins: Plugin[]) => void
}

export const MarketplaceContext = createContext<MarketplaceContextValue>({
  intersected: true,
  setIntersected: () => {},
  searchPluginText: '',
  handleSearchPluginTextChange: () => {},
  filterPluginTags: [],
  handleFilterPluginTagsChange: () => {},
  activePluginType: PLUGIN_TYPE_SEARCH_MAP.all,
  handleActivePluginTypeChange: () => {},
  plugins: undefined,
  setPlugins: () => {},
})

type MarketplaceContextProviderProps = {
  children: ReactNode
}

export function useMarketplaceContext(selector: (value: MarketplaceContextValue) => any) {
  return useContextSelector(MarketplaceContext, selector)
}

export const MarketplaceContextProvider = ({
  children,
}: MarketplaceContextProviderProps) => {
  const [intersected, setIntersected] = useState(true)
  const [searchPluginText, setSearchPluginText] = useState('')
  const [filterPluginTags, setFilterPluginTags] = useState<string[]>([])
  const [activePluginType, setActivePluginType] = useState(PLUGIN_TYPE_SEARCH_MAP.all)
  const [plugins, setPlugins] = useState<Plugin[]>()

  const handleUpdatePlugins = useCallback(async (query: PluginsSearchParams) => {
    const { marketplacePlugins } = await getMarketplacePlugins(query)

    setPlugins(marketplacePlugins)
  }, [])

  const { run: handleUpdatePluginsWithDebounced } = useDebounceFn(handleUpdatePlugins, {
    wait: 500,
  })

  const handleSearchPluginTextChange = useCallback((text: string) => {
    setSearchPluginText(text)

    handleUpdatePluginsWithDebounced({ query: text })
  }, [handleUpdatePluginsWithDebounced])

  const handleFilterPluginTagsChange = useCallback((tags: string[]) => {
    setFilterPluginTags(tags)
  }, [])

  const handleActivePluginTypeChange = useCallback((type: string) => {
    setActivePluginType(type)
  }, [])

  return (
    <MarketplaceContext.Provider
      value={{
        intersected,
        setIntersected,
        searchPluginText,
        handleSearchPluginTextChange,
        filterPluginTags,
        handleFilterPluginTagsChange,
        activePluginType,
        handleActivePluginTypeChange,
        plugins,
        setPlugins,
      }}
    >
      {children}
    </MarketplaceContext.Provider>
  )
}