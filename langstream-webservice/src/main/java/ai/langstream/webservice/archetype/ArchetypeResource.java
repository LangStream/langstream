/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.webservice.archetype;

import ai.langstream.webservice.security.infrastructure.primary.TokenAuthFilter;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.NotBlank;
import java.util.Collection;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Tag(name = "archetypes")
@RequestMapping("/api/archetypes")
@Slf4j
@AllArgsConstructor
public class ArchetypeResource {

    ArchetypeService archetypeService;

    private void performAuthorization(Authentication authentication, final String tenant) {
        if (authentication == null) {
            return;
        }
        if (!authentication.isAuthenticated()) {
            throw new IllegalStateException();
        }
        if (authentication.getAuthorities() != null) {
            final GrantedAuthority grantedAuthority =
                    authentication.getAuthorities().stream()
                            .filter(
                                    authority ->
                                            authority
                                                    .getAuthority()
                                                    .equals(TokenAuthFilter.ROLE_ADMIN))
                            .findFirst()
                            .orElse(null);
            if (grantedAuthority != null) {
                return;
            }
        }
        if (authentication.getPrincipal() == null) {
            throw new IllegalStateException();
        }
        final String principal = authentication.getPrincipal().toString();
        if (tenant.equals(principal)) {
            return;
        }
        throw new ResponseStatusException(HttpStatus.FORBIDDEN);
    }

    @GetMapping("/{tenant}")
    @Operation(summary = "Get all archetypes")
    Collection<String> getArchetypes(
            Authentication authentication, @NotBlank @PathVariable("tenant") String tenant) {
        performAuthorization(authentication, tenant);
        return archetypeService.getAllArchetypes(tenant);
    }

    @GetMapping("/{tenant}/{id}")
    @Operation(summary = "Get metadata about an archetype")
    ArchetypeDefinition getArchetype(
            Authentication authentication,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("id") String id) {
        performAuthorization(authentication, tenant);
        ArchetypeDefinition result = archetypeService.getArchetype(tenant, id);
        if (result == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
        return result;
    }
}
