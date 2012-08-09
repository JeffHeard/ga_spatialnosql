import shutil
from ga_spatialnosql.db.mongo import GeoJSONCollection
from django.http import HttpResponseRedirect
from django.views import generic as g
from django import forms as f
from django.forms import widgets as w
from tempfile import NamedTemporaryFile, mkdtemp
from osgeo import ogr, osr
from django.conf import settings
import shutil
import os
import zipfile
import tarfile
import json
import re

def normalize(name):
    name = re.sub('%', 'pct', name)
    name = re.sub('[^A-z0-9_]', '_', name).lower()
    if name[0] in '0123456789_':
        name = 'x' + name
    name = re.sub('__*', "_", name)
    name = re.sub('__*$', '', name)
    return name

class UploadFileForm(f.Form):
    collection_name = f.CharField()
    uploaded_file = f.FileField()
    srid = f.IntegerField()
    master_file = f.CharField(min_length=0)

class SubmitJSONForm(f.Form):
    collection_name = f.CharField()
    geojson = f.CharField(widget=w.Textarea)
    srid = f.IntegerField()


class SubmitJSONView(g.FormView):
    form_class = SubmitJSONForm
    template_name = 'ga_spatialnosql/submit_json.template.html'
    success_url = '.'

    def form_valid(self, form):
        conn = settings.MONGODB_CONNECTIONS['ga_spatialnosql'] if 'ga_spatialnosql' in settings.MONGODB_CONNECTIONS else settings.MONGODB_CONNECTIONS['default']
        db = conn[self.request.user.username]
        fc = json.loads(form.cleaned_data['geojson'])
        GeoJSONCollection(
            db=db,
            collection=form.cleaned_data['collection_name'],
            srid=form.cleaned_data['srid'],
            fc=fc,
            clear=True
        )
        return HttpResponseRedirect(self.success_url)

class UploadOGRView(g.FormView):
    form_class = UploadFileForm
    template_name = 'ga_spatialnosql/submit_ogr.template.html'
    success_url = '.'

    def form_valid(self, form):
        conn = settings.MONGODB_CONNECTIONS['ga_spatialnosql'] if 'ga_spatialnosql' in settings.MONGODB_CONNECTIONS else settings.MONGODB_CONNECTIONS['default']
        db = conn[self.request.user.username]

        tf = NamedTemporaryFile()
        for chunk in form.cleaned_data['uploaded_file'].chunks():
            tf.write(chunk)
        tf.flush()
        filename = tf.name

        dir = None
        if form.cleaned_data['uploaded_file'].name.endswith('.zip'):
            pwd = os.getcwd()
            dir = mkdtemp()
            os.chdir(dir)
            archive = zipfile.ZipFile(filename)
            archive.extractall()
            ds = ogr.Open(form.cleaned_data['master_file'])
            os.chdir(pwd)
        elif form.cleaned_data['uploaded_file'].name.endswith('.tar.gz'):
            pwd = os.getcwd()
            dir = mkdtemp()
            os.chdir(dir)
            archive = tarfile.TarFile.gzopen(filename)
            archive.extractall()
            ds = ogr.Open(form.cleaned_data['master_file'])
            os.chdir(pwd)
        elif form.cleaned_data['uploaded_file'].name.endswith('.tar.bz2'):
            pwd = os.getcwd()
            dir = mkdtemp()
            os.chdir(dir)
            archive = tarfile.TarFile.bz2open(filename)
            archive.extractall()
            ds = ogr.Open(form.cleaned_data['master_file'])
            os.chdir(pwd)
        else:
            ds = ogr.Open(filename)

        t_srs = osr.SpatialReference()
        t_srs.ImportFromEPSG(form.cleaned_data['srid'])

        if ds.GetLayerCount() == 1:
            layer = ds.GetLayer(0)
            s_srs = layer.GetSpatialRef()
            crx = osr.CoordinateTransformation(s_srs, t_srs)
            l = [f for f in layer]
            for f in l:
                print ""
                print f.geometry().ExportToWkt()
                f.geometry().Transform(crx)
                print f.geometry().ExportToWkt()
            name = normalize(layer.GetName())
            GeoJSONCollection(
                index_path=settings.INDEX_PATH,
                db=db,
                collection = form.cleaned_data['collection_name'] + '__' + name,
                srid=form.cleaned_data['srid'],
                fc={ "type" : "FeatureCollection", 'features' : [json.loads(f.ExportToJson()) for f in l] },
                clear=True
            )
        else:
            for layer in (ds.GetLayerByIndex(i) for i in range(ds.GetLayerCount())):
                name = normalize(layer.GetName())
                s_srs = layer.GetSpatialRef()
                crx = osr.CoordinateTransformation(s_srs, t_srs)
                l = [f for f in layer]
                for f in l:
                    f.geometry().Transform(crx)
                GeoJSONCollection(
                    index_path=settings.INDEX_PATH,
                    db=db,
                    collection = form.cleaned_data['collection_name'] + '__' + name,
                    srid=form.cleaned_data['srid'],
                    fc=(json.loads(f.ExportToJson()) for f in l),
                    clear=True
                )

        if dir:
            shutil.rmtree(dir)
        return HttpResponseRedirect(self.success_url)


class CollectionView(g.View):
    def get(self, request, *args, **kwargs):
        pass

    def post(self, request, *args, **kwargs):
        pass

    def put(self, request, *args, **kwargs):
        pass

    def delete(self, request, *args, **kwargs):
        pass


class CollectionListView(g.View):
    def get(self, request, *args, **kwargs):
        pass

    def post(self, request, *args, **kwargs):
        pass

    def put(self, request, *args, **kwargs):
        pass

    def delete(self, request, *args, **kwargs):
        pass

