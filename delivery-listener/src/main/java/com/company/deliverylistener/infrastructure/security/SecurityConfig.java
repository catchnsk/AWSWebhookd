package com.company.deliverylistener.infrastructure.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Spring Security configuration.
 *
 * <p>Security model:
 * <ul>
 *   <li>Actuator health and info endpoints: open (required by EKS readiness/liveness probes).</li>
 *   <li>Prometheus scrape endpoint ({@code /actuator/prometheus}): open for cluster-internal scraping;
 *       restrict via network policy in production.</li>
 *   <li>Admin/operations API endpoints ({@code /api/**}): protected by HTTP Basic Auth.</li>
 *   <li>CSRF is disabled – this is a stateless REST/Kafka service, not a browser app.</li>
 * </ul>
 *
 * <p>Admin credentials are supplied via environment variables / Kubernetes Secrets:
 * <pre>
 *   ADMIN_USERNAME (default: admin)
 *   ADMIN_PASSWORD (BCrypt-encoded; must be set in Kubernetes Secret)
 * </pre>
 *
 * <p>For production, replace in-memory user store with an IAM-integrated JWT validator
 * or internal mTLS client certificate auth.
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Value("${security.admin.username:admin}")
    private String adminUsername;

    @Value("${security.admin.password:{noop}changeme}")
    private String adminPassword;

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            .csrf(AbstractHttpConfigurer::disable)
            .authorizeHttpRequests(auth -> auth
                // EKS probe endpoints – must be open
                .requestMatchers(
                    "/actuator/health",
                    "/actuator/health/**",
                    "/actuator/info").permitAll()
                // Prometheus scraping – restrict via NetworkPolicy in production
                .requestMatchers("/actuator/prometheus").permitAll()
                // All other actuator endpoints require admin role
                .requestMatchers("/actuator/**").hasRole("ADMIN")
                // Operations API
                .requestMatchers("/api/**").hasRole("ADMIN")
                .anyRequest().authenticated()
            )
            .httpBasic(basic -> {});

        return http.build();
    }

    @Bean
    public UserDetailsService userDetailsService(PasswordEncoder encoder) {
        // In production, load from Kubernetes Secret or AWS Secrets Manager
        var admin = User.builder()
                .username(adminUsername)
                .password(adminPassword)
                .roles("ADMIN")
                .build();
        return new InMemoryUserDetailsManager(admin);
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }
}
