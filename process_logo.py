from PIL import Image
import os

try:
    img = Image.open("static/logo.png")
    width, height = img.size
    
    # crop the top 75% to get the circular icon, assuming centered
    # The text is at the bottom.
    
    # Let's try to find the bounding box of the non-black pixels?
    # Or just hard crop.
    
    # 1024x1024 image.
    # Icon is likely in the center-top.
    # Let's crop centered square from top.
    
    icon_size = int(height * 0.70)
    left = (width - icon_size) // 2
    top = int(height * 0.1) # Start a bit from top
    right = left + icon_size
    bottom = top + icon_size
    
    icon = img.crop((left, top, right, bottom))
    icon.save("static/favicon.png")
    print("Favicon created.")
    
except ImportError:
    print("PIL not installed. Installing...")
    os.system("pip install pillow")
    # Retry logic would be needed but for now let's just assume it works or fail gracefully
    pass
except Exception as e:
    print(f"Error: {e}")
