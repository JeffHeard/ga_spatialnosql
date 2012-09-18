from django.db import models
import hmac

# Create your models here.
import uuid
from hashlib import sha1
from django.contrib.auth.models import User

class ApiKey(models.Model):
    user = models.OneToOneField(User, related_name='api_key')
    key = models.CharField(max_length=256, blank=True, default='')
    collection = models.CharField(max_length=255)
    created = models.DateTimeField(auto_now_add=True)
    read = models.BooleanField(default=True)
    write = models.BooleanField(default=False)
    delete = models.BooleanField(default=False)
    modify_properties = models.BooleanField(default=False)

    def __unicode__(self):
        return u"%s for %s" % (self.key, self.user)

    def save(self, *args, **kwargs):
        if not self.key:
            self.key = self.generate_key()

        return super(ApiKey, self).save(*args, **kwargs)

    def generate_key(self):
        # Get a random UUID.
        new_uuid = uuid.uuid4()
        # Hmac that beast.
        return hmac.new(str(new_uuid), digestmod=sha1).hexdigest()


def create_api_key(sender, **kwargs):
    """
    A signal for hooking up automatic ``ApiKey`` creation.
    """
    if kwargs.get('created') is True:
        ApiKey.objects.create(user=kwargs.get('instance'))


class ApiKeyAuthorization(object):
    def __init__(self, collection, request):
        self.collection = collection
        self.request = request

    def authorize(self):
        """This assumes the user is already authenticated.  Instead, what we focus on is establishing authorization for
        particular levels of service.  Here we fetch the ApiKey object and cache it for retrieval later and associate it
        with an auth token that is only good for this session.  We return that auth token, and finding the authorization
        again requires that the user submit the auth token as part of the request.
        """
        if self.request.user.is_authenticated() and 'api_key' in self.request.REQUEST:
            api_keycode = self.request.REQUEST['api_key']
            self.api_key = ApiKey.objects.get(user=self.request.user, key=api_keycode)
            auth_token = uuid.uuid4()
            auth_hash = hmac.new(str(auth_token), digestmod=sha1).hexdigest()
            self.request.session[auth_hash] = self
            self.request.session.save()
            return auth_token

    @property
    def write(self):
        return self.api_key.write

    @property
    def read(self):
        return self.api_key.read

    @property
    def delete(self):
        return self.api_key.delete

    @property
    def modify_properties(self):
        return self.api_key.modify_properties