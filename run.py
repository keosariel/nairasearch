from cronus import Cronus
import  re
config  = {
            "pass":[
                re.compile("https://www.nairaland.com/[A-Za-z0-9-]+/[0-9]+"),
                re.compile("https://www.nairaland.com/[0-9]+/[A-Za-z0-9-]+/[0-9]+"),
                re.compile("https://www.nairaland.com/hopto/home/[0-9]+"),
                # re.compile("https://www.nairaland.com/[A-Za-z0-9]+")
                ],
            "seed":["https://www.nairaland.com/"],
            "working_dir":"nairaland",
            "limit_urls":200,
			"recent": [
				"https://www.nairaland.com/",
				"https://www.nairaland.com/politics",
				"https://www.nairaland.com/jobs",
				"https://www.nairaland.com/crime",
				"https://www.nairaland.com/nairaland",
				"https://www.nairaland.com/business",
				"https://www.nairaland.com/education",
				"https://www.nairaland.com/events",
				"https://www.nairaland.com/sports",
				"https://www.nairaland.com/techmarket",
				"https://www.nairaland.com/science",
				"https://www.nairaland.com/entertainment"
			]
}

c = Cronus(**config)
c.run()
