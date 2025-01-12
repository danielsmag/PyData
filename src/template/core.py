"""
Core functionality module for the template package.

This module contains the main functions and utilities that form
the core functionality of the template package.
"""

def hello(name: str) -> None:
    """
    Print a greeting message with the given name.

    Args:
        name (str): The name to include in the greeting message.
            Can be any non-empty string.

    Raises:
        TypeError: If name is not a string.

    Examples:
        >>> hello("Alice")
        Hello Alice
        
        >>> hello("")
        Hello 
    """
    if not isinstance(name, str):
        raise TypeError("name must be a string")
    print(f"Hello {name}")
