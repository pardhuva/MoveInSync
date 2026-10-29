package com.moveinsync.livetrips.config;

import com.moveinsync.livetrips.web.PrincipalArgumentResolver;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

/** Registers the resolver that turns a bearer token into an authenticated principal. */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    private final PrincipalArgumentResolver principalResolver;

    public WebMvcConfig(PrincipalArgumentResolver principalResolver) {
        this.principalResolver = principalResolver;
    }

    @Override
    public void addArgumentResolvers(List<HandlerMethodArgumentResolver> resolvers) {
        resolvers.add(principalResolver);
    }
}
