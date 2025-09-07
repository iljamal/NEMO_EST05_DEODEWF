import pystac
import xarray as xr
catalog = pystac.Catalog.from_file("https://object-store.os-api.cci1.ecmwf.int/deode-dcmdb/stac-catalog/dcmdb-stac-catalog/dcmdb-stac-catalog.json")
collection=list(catalog.get_collections())[0]
item = collection.get_item('z4EBG9jGwBD5uQn4XBeH6P1rpaUQshnrz13m9D4HQtwmPgGoDcx')
asset = item.assets['ECFS data']
ds = xr.open_datatree(asset.href, **asset.extra_fields['xarray:open_kwargs'])

print(ds)