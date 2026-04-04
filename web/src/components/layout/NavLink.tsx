import React from 'react';
import { NavLink as RouterNavLink, NavLinkProps as RouterNavLinkProps } from 'react-router-dom';
import { cn } from '../../utils/cn';

export interface NavLinkProps extends RouterNavLinkProps {
  className?: string;
  activeClassName?: string;
}

export function NavLink({ className, activeClassName, ...props }: NavLinkProps) {
  return (
    <RouterNavLink
      {...props}
      className={({ isActive }) => cn(
        'text-[13px] text-text-secondary transition-colors hover:text-text-primary hover:underline underline-offset-4 decoration-border-strong rounded-sm outline-none focus-visible:ring-2 focus-visible:ring-indigo-500',
        isActive && cn('text-text-primary', activeClassName),
        className
      )}
    />
  );
}
