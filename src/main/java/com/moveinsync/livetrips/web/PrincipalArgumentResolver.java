package com.moveinsync.livetrips.web;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.auth.JwtService;
import com.moveinsync.livetrips.error.Errors;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.core.MethodParameter;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

/**
 * Makes an endpoint's authentication requirement visible in its signature: a controller method
 * that declares an {@link AuthenticatedPrincipal} parameter can only be reached with a valid
 * bearer token, because this resolver throws otherwise.
 *
 * <p>Chosen over a servlet filter because a filter has to be told which paths are public, and that
 * list inevitably drifts from reality. Here the default is closed and the requirement is stated at
 * the only place that can't fall out of sync with the handler.
 *
 * <p>Tokens are read only from the {@code Authorization} header, never from a query parameter, so
 * a credential never lands in an access log or browser history.
 */
@Component
public class PrincipalArgumentResolver implements HandlerMethodArgumentResolver {

    private final JwtService jwtService;

    public PrincipalArgumentResolver(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return AuthenticatedPrincipal.class.equals(parameter.getParameterType());
    }

    @Override
    public Object resolveArgument(MethodParameter parameter,
                                  ModelAndViewContainer mavContainer,
                                  NativeWebRequest webRequest,
                                  WebDataBinderFactory binderFactory) {
        HttpServletRequest request = webRequest.getNativeRequest(HttpServletRequest.class);
        if (request == null) {
            throw Errors.unauthenticated();
        }
        String token = JwtService.bearerToken(request.getHeader("Authorization"));
        if (token == null) {
            throw Errors.unauthenticated();
        }
        return jwtService.verify(token);
    }
}
