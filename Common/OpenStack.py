#!/usr/bin/env python
# -*- coding: utf-8 -*-


class OpenStack(object):

    def __init__(self, log,
                       yaml_data=None,
                       distribution=None,
                       api_version=2,
                       site_packages_path='lib/python2.7/site-packages/'):
        self.log = log
        self.yaml_data = yaml_data
        self.distribution = distribution
        self.api_version = api_version
        self.site_packages_path = site_packages_path

        self.user_name = None
        self.password = None
        self.tenant_name = None
        self.cacert_path = None
        self.auth_url = None

        self.cinder_client = None

    def set_cinder_client(self, user_name=None,
                                password=None,
                                tenant_name=None,
                                cacert_path=None,
                                auth_url=None,
                                timeout=120,
                                endpoint_type='internalURL'):
        if self.yaml_data is None:
            self.user_name = user_name
            self.password = password
            self.tenant_name = tenant_name
            self.cacert_path = cacert_path
            self.auth_url = auth_url

            self.log.info("user_name = %s\n"
                          "password = %s\n"
                          "tenant_name = %s\n"
                          "cacert_path = %s\n"
                          "auth_url = %s"
                          % (self.user_name,
                             self.password,
                             self.tenant_name,
                             self.cacert_path,
                             self.auth_url))
        else:
            self.user_name = self.yaml_data['user_name']
            self.password = self.yaml_data['password']
            self.tenant_name = self.yaml_data['tenant_name']
            self.cacert_path = self.yaml_data['cacert_path']
            self.auth_url = self.yaml_data['auth_url']

            self.log.info(self.yaml_data)

        # import cinderclient
        try:
            if self.distribution is None:
                from cinderclient import client
            elif self.distribution == "helion":
                helion_cindercleint = glob.glob("/opt/stack/venv/cinderclient*")
                if len(helion_cinderclient) == 0:
                    raise ImportError("Error, unable to import helion cindercleint.")
                helion_path = os.path.normpath(os.path.join(helion_cindercleint[0],
                                                            self.site_packages_path))
                self.log.debug("add helion path %s" % helion_path)
                sys.path = [helion_path] + sys.path
                from cinderclient import client
            else:
                self.log.error("unknown openstack distribution. %s" % self.distribution)
                return False

            self.log.debug("from cinderclient import client")
        except:
            self.log.error("unable to import cinder client.")
            return False

        # set cinder client
        try:
            if os.path.exists(cacert_path):
                self.cinder_client = client.Client(self.api_version,
                                                   self.user_name,
                                                   self.password,
                                                   self.tenant_name,
                                                   self.auth_url,
                                                   cacert=self.cacert_path,
                                                   verify=False,
                                                   endpoint_type=endpoint_type)
            else:
                self.cinder_client = client.Client(self.api_version,
                                                   self.user_name,
                                                   self.password,
                                                   self.tenant_name,
                                                   self.auth_url,
                                                   timeout=timeout,
                                                   insecure=True,
                                                   verify=False,
                                                   endpoint_type=endpoint_type)
            return True
        except:
            self.log.error("unable to set cinder client ")
            return False

    def _cinder_volumes(self):
        try:
            self.log.debug("get cinder volume list.")
            return self.cinder_client.volumes.list()
        except:
            self.log.error("unable to get cinder volume list.")
            return False

    def get_cinder_volume(self, only_yaml_volumes=True):
        '''
        if only_yaml_volumes is true, get volume info specified in yaml file only.
        return dict of volume name and id. { 'name': 'id', ... }
        '''
        cinder_volumes = self._cinder_volumes()
        try:
            volume_map = {}
            if only_yaml_volumes:
                map_volumes = self.yaml_data[volume_names]
                for cinder_volume in cinder_volumes:
                    if cinder_volume.name in map_volumes:
                        self.log.info("map %s => %s" % (cinder_volume.name('ascii'), cinder_volume.id.encode('ascii')))
                        volume_map[cinder_volume.name.encode('ascii')] = cinder_volume.id.encode('ascii')
                    else:
                        self.log.warning("unable to map %s" % cinder_volume.name('ascii'))
            else:
                for cinder_volume in cinder_volumes:
                    self.log.info("map %s => %s" % (cinder_volume.name('ascii'), cinder_volume.id.encode('ascii')))
                    volume_map[cinder_volume.name.encode('ascii')] = cinder_volume.id.encode('ascii')
            return volume_id_list
        except:
            self.log.error("unable to map volume name and id.")
            return False
