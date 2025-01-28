from dependency_injector import containers, providers


class ApplicationContainer(containers.DeclarativeContainer):
    
    config  = providers.Configuration()
    app_settings = providers.Singleton(provides=get_settings)
   